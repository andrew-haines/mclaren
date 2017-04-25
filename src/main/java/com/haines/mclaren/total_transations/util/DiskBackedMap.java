package com.haines.mclaren.total_transations.util;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.haines.mclaren.total_transations.io.ByteBufferInputStream;
import com.haines.mclaren.total_transations.io.ByteBufferOutputStream;

//@NotThreadSafe
public class DiskBackedMap<K extends Serializable, V extends SimpleMap.Keyable<K>> implements SimpleMap<K, V>{

	private final BucketBuffers<K, V> bucketBuffers;
	private Map<K, V> currentlyLoadedBucket;
	private int loadedBucketNumber;
	
	DiskBackedMap(BucketBuffers<K, V> bucketBuffers) throws ClassNotFoundException, IOException{
		this.bucketBuffers = bucketBuffers;
		this.currentlyLoadedBucket = new HashMap<K, V>();
		this.loadedBucketNumber = 0;
	}
	
	private void loadBucket(int bucketNum) throws IOException, ClassNotFoundException {
		
		// save the current bucket
		
		bucketBuffers.saveBucket(loadedBucketNumber, currentlyLoadedBucket);
		
		// load the required bucket
		currentlyLoadedBucket = bucketBuffers.loadBucket(bucketNum);
		
		loadedBucketNumber = bucketNum;
	}

	@Override
	public V get(K key) {
		loadBucketForKey(key);
		
		return currentlyLoadedBucket.get(key);
	}

	private void loadBucketForKey(K key) {
		int bucketNumber = bucketBuffers.getBucketNumForKey(key);
		
		if (bucketNumber != loadedBucketNumber){
			try {
				loadBucket(bucketNumber);
			} catch (ClassNotFoundException | IOException e) {
				throw new RuntimeException("unable to load bucket: "+bucketNumber, e);
			}
		}
	}

	@Override
	public V put(K key, V value) {
		loadBucketForKey(key);
		
		return currentlyLoadedBucket.put(key, value);
	}

	@Override
	public long size() {
		
		/* calculated by taking the total amount of persisted entries, minus the current loaded persisted
		 * bucket size plus the in memory bucket size.
		 */
		return bucketBuffers.getTotalElements() - bucketBuffers.getSizeOfPersistedBucket(loadedBucketNumber) + currentlyLoadedBucket.size();
	}
	
	@Override
	public Iterable<V> getAllValues(){
		
		// manually constructing the iterable for all elements as the Stream based implementation is painful to follow.
		return new Iterable<V>(){ 

			private Iterator<Integer> nodeIdIt = bucketBuffers.bucketNodes.keySet().iterator();
			private Iterator<V> currentIt = currentlyLoadedBucket.values().iterator();
			
			@Override
			public Iterator<V> iterator() {
				return new Iterator<V>(){

					@Override
					public boolean hasNext() {
						boolean hasNext = currentIt.hasNext();
						
						if (!hasNext){
							hasNext = nodeIdIt.hasNext();
							if (hasNext){
								// load next node into memory
								
								int nextNodeId = nodeIdIt.next();
								try {
									
									Map<K, V> nextDataNode = bucketBuffers.loadBucket(nextNodeId);
									
									currentIt = nextDataNode.values().iterator();
								} catch (ClassNotFoundException | IOException e) {
									throw new RuntimeException("Unable to load node id: "+nextNodeId, e);
								}
							}
						}
						
						return hasNext;
					}

					@Override
					public V next() {
						
						return currentIt.next();
					}
					
				};
			}
		};
	}
	
	@Override
	public Stream<V> processAllEvents(Stream<V> events, BiFunction<V, V, V> reduceFunction) {
		/*
		 *  calculate the buckets of all events and group the events by their buckets and then order them, with the function
		 *  applied, to limit the iteration to involve the minimum bucket access. In other words, using this we only ever
		 *  access each bucket once per Iterable iteration.
		 *  
		 *  Unfortunately we can't combine this with the next stream based command as we need to first pass
		 *  over the items in order to group them by their appropriate bucket ids. This is a small price to pay
		 *  to avoid potentially making big IO pulls for each event.
		 */
		Map<Integer, List<V>> buckets = groupIntoBuckets(events, bucketBuffers);
		
		return buckets.values().stream()
				.flatMap(be -> be.stream()
									.map(e -> reduceFunction.apply(e, get(e.getKey())))
				);
	}
	
	private static <K extends Serializable, V1 extends Keyable<K> & Serializable> Map<Integer, List<V1>> groupIntoBuckets(Stream<V1> events, BucketBuffers<K, V1> bucketBuffers) {
		return events
				.collect(Collectors.groupingBy(e -> bucketBuffers.getBucketNumForKey(e.getKey())));
	}
	
	public void close() throws IOException {
		// ensure current buffer is persisted
		
		bucketBuffers.saveBucket(loadedBucketNumber, currentlyLoadedBucket);
	}

	/**
	 * An abstraction that works like a file system based tree where directories are branching nodes and files are
	 * serialized contents of the memory units stored in that node. Branches have to be rebalanced when a memory unit
	 * gets too big and needs to be split. This is a major performance overhead and really should be performed on a separate
	 * thread rather than blocking the client thread. As the current system design has this performed on a separate thread
	 * anyway this shouldn't be too much of an issue. Note that the searching for each node works using hashes from the key
	 * multipled by the depth of the node mod-ed to the branching factor. This way any re balancing when a memory unit gets too
	 * big is isolated to the node of the existing memory unit. The indexing is performed purely on the hash of the key 
	 * to determine the bucket at each node rather than any binary search that would become invalidated when the tree structure
	 * changes.
	 * 
	 * Another way of making this better is to either pre-emptively determine the
	 * tree structure but this requires prior knowledge of the distribution of the keys - something we are unlikely to
	 * have knowledge of.
	 * 
	 * Note that, like the majority of code in this project, this class is inherritently not thread safe so that any
	 * multithreaded access has to be managed by the caller. 
	 * 
	 * @author haines
	 *
	 * @param <K>
	 * @param <V>
	 */
	static class BucketBuffers<K extends Serializable, V extends Serializable> {

		private final static int DEFAULT_BRANCHING_SIZE = 8;
		
		private long totalElements; // unlikely to need more than 32 bits (2B items) but is this is big data distributed over many many nodes, this is very possible
		private final Map<Integer, Integer> bucketSizes;
		private final Map<Integer, Node> bucketNodes;
		private final Node head;
		private final int maxElementsInMemoryUnit;
		private int maxBucketId;
		
		BucketBuffers(Path rootFolder, int maxElementsInMemoryUnit){
			this(DEFAULT_BRANCHING_SIZE, rootFolder, maxElementsInMemoryUnit);
		}
		
		public long getSizeOfPersistedBucket(int loadedBucketNumber) {
			// TODO Auto-generated method stub
			return 0;
		}

		private BucketBuffers(int branchingSize, Path rootFolder, int maxElementsInMemoryUnit){
			this.bucketSizes = new HashMap<Integer, Integer>();
			this.bucketNodes = new HashMap<Integer, Node>();
			this.totalElements = 0;
			this.head = new Node(0, branchingSize, createNewNodeFile(0, rootFolder), 0);
			this.maxElementsInMemoryUnit = maxElementsInMemoryUnit;
			this.maxBucketId = 0;
		}
		
		/**
		 * Create a new id file under the supplied root.
		 * @param idNumber
		 * @param rootFolder
		 * @return
		 */
		private static Path createNewNodeFile(int idNumber, Path rootFolder) {
			
			String idFileName = getFileName(idNumber);
			
			if (!Files.isDirectory(rootFolder)){
				throw new IllegalArgumentException("the supplied root folder: "+rootFolder+" is not a directory. Unable to create file: "+idFileName);
			}
			
			Path idFile = Paths.get(rootFolder.toString(), idFileName);
			
			return idFile;
		}

		private static String getFileName(int idNumber) {
			return idNumber+".dat";
		}

		public int getBucketNumForKey(K key){
			return getBucketNumForKey(head, key);
		}
		
		public int getBucketNumForKey(Node node, K key){
			if (head.getNumChildren() == 0){
				// we have a terminal node which is a memory unit.
				
				return head.id;
			} else{
				// This is a branching node, re hash this again based a prime distributed depth and recurse
				
				int newHash = 31 * head.depth + key.hashCode();
				
				int branchBucket = newHash % head.getNumChildren();
				
				return getBucketNumForKey(head.getChild(branchBucket), key);
				
				
			}
		}
		
		/** Returns the total number of elements that have been persisted in the buckets
		 * 
		 * @return
		 */
		public long getTotalElements() {
			return totalElements; 
		}

		public Node getBucketNode(int bucketNum){
			return bucketNodes.get(bucketNum);
		}

		public Path getBucketFile(int bucketNum) {
			return getBucketNode(bucketNum).fileLocation;
		}

		public Map<K, V> loadBucket(int bucketNum) throws ClassNotFoundException, IOException {
			return loadBucket(getBucketFile(bucketNum));
		}

		public void saveBucket(int loadedBucketNumber, Map<K, V> currentlyLoadedBucket) throws IOException {
			saveBucket(getBucketNode(loadedBucketNumber), currentlyLoadedBucket);
			
			// update bucket sizes
			
			int newBucketSize = currentlyLoadedBucket.size();
			
			Integer previousBucketSize = bucketSizes.put(loadedBucketNumber, newBucketSize);
			
			if (previousBucketSize != null){
				totalElements -= previousBucketSize;
			}
			
			totalElements += newBucketSize;
		}
		
		private void saveBucket(Node bucketNode, Map<K, V> bucketContents) throws IOException{
			
			if (bucketContents.size() < maxElementsInMemoryUnit){ // just persist to the current node as a memory file.
				FileChannel channel = FileChannel.open(bucketNode.fileLocation, StandardOpenOption.WRITE, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
				
				ByteBuffer buffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, channel.size());
				
				try(ObjectOutputStream out = new ObjectOutputStream(new ByteBufferOutputStream(buffer))){
					out.writeObject(bucketContents);
				}
			} else {
				
				// the bucket memory unit has got too big. promote to branching node and add the contents of
				// the memory node to data across the 
				
				maxBucketId = bucketNode.promoteToBranchNode(maxBucketId);
				
				// add into the node cache
				
				for (int i = 0; i < bucketNode.getNumChildren(); i++){
					Node newNode = bucketNode.getChild(i);
					
					Node existingNode = bucketNodes.put(newNode.id, newNode);
					
					if (existingNode != null){
						throw new IllegalStateException("Trying to insert a new node: "+newNode.toString()+" but there is already node indexed: "+existingNode);
					}
				}
				
				// now iterate over the children and add the bucket contents to the appropriate bucket it hashes
				// to based on this depth
				
				Map<Integer, List<Entry<K, V>>> buckets = groupIntoBuckets(bucketNode, bucketContents.entrySet(), this);
				
				buckets.entrySet().stream()
					.forEach(be -> {
						try {
							saveBucket(bucketNode.getChild(be.getKey()), be.getValue().stream()
																					  .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue())));
						} catch (IOException e1) {
							throw new RuntimeException("Unable to save bucket branch : "+be.getKey()+" from node: "+bucketNode, e1);
						}
					});
			}
		}
		
		private static <K extends Serializable, V extends Serializable> Map<Integer, List<Entry<K, V>>> groupIntoBuckets(Node parentNode, Collection<? extends Entry<K, V>> events, BucketBuffers<K, V> bucketBuffers) {
			return StreamSupport.stream(events.spliterator(), false)
					.collect(Collectors.groupingBy(e -> bucketBuffers.getBucketNumForKey(parentNode, e.getKey())));
		}
		
		@SuppressWarnings("unchecked")
		private Map<K, V> loadBucket(Path bucketFile) throws IOException, ClassNotFoundException{
			
			FileChannel channel = FileChannel.open(bucketFile, StandardOpenOption.READ);
			
			ByteBuffer buffer = channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size());
			
			try(ObjectInputStream in = new ObjectInputStream(new ByteBufferInputStream(buffer))){
			
				return (Map<K, V>)in.readObject();
			}
		}
		
		private static class Node {
			
			private final int id;
			private final Node[] children;
			private int numChildren;
			private final Path fileLocation;
			private final int depth;
			
			private Node(int id, int branchingSize, Path fileLocation, int depth){
				
				assert id >= 0;
				assert branchingSize > 0;
				assert fileLocation != null;
				assert depth >= 0;
				
				this.id = id;
				this.children = new Node[branchingSize];
				this.numChildren = 0;
				this.fileLocation = fileLocation;
				this.depth = depth;
			}

			public Node getChild(int branchBucket) {
				assert numChildren > 0;
				
				return children[branchBucket];
			}

			public int getNumChildren() {
				return numChildren;
			}
			
			public int promoteToBranchNode(int maxId) throws IOException{
				assert numChildren == 0;
				numChildren = children.length;
				
				// delete the existing file and change it to a directory. This file will be loaded in memory already
				
				Files.delete(fileLocation);
				Files.createDirectory(fileLocation);
				
				int newDepth = depth + 1;
				
				for (int i = 0; i < children.length; i++){
					maxId += i;
					
					children[i] = new Node(maxId, children.length, BucketBuffers.createNewNodeFile(maxId, fileLocation), newDepth);
				}
				
				return maxId;
			}
		}
	}
}