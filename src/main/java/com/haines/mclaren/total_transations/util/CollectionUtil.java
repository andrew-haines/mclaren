package com.haines.mclaren.total_transations.util;

import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;
import com.haines.mclaren.total_transations.io.Util;

public class CollectionUtil {

	private static final Logger LOG = Logger.getLogger(CollectionUtil.class.getName());
	
	//@NotThreadSafe
	public static <T> Iterable<T> cycle(final Iterable<T> it){
		return new Iterable<T>(){

			public Iterator<T> iterator() {
				return new Iterator<T>(){

					private Iterator<T> nextCycle = it.iterator();
					
					public boolean hasNext() {
						return true; // forever cycles
					}

					public T next() {
						if (!nextCycle.hasNext()){
							nextCycle = it.iterator(); // create the next cycle
						}
						return nextCycle.next();
					}
				};
			}
		};
	}
	
	/**
	 * Creates a file backed map that keeps the MRU entries in memory
	 * @param localFile
	 * @param maximumItems
	 * @param comparator
	 * @return
	 * @throws IOException
	 * @throws ClassNotFoundException 
	 */
	public static <K extends Serializable, V extends SimpleMap.Keyable<K>> SimpleMap<K, V> getFileBackedMap(Path localFile, int maximumInMemoryItems) throws IOException, ClassNotFoundException{
		
		return new DiskBackedMap<K, V>(createBucketBuffer(localFile, maximumInMemoryItems));
	}
	
	private static <K extends Serializable, V extends Serializable> DiskBackedMap.BucketBuffers<K, V> createBucketBuffer(Path localFile, int maxEntriesInMemoryUnit) throws IOException {
		
		if(Files.exists(localFile)){
			Util.recursiveDelete(localFile);
			
		}
		
		localFile = Files.createDirectory(localFile);
		
		if (!Files.isDirectory(localFile)){
			throw new IllegalArgumentException("Path: "+localFile+" is not a directory");
		}
		
		LOG.log(Level.INFO, "clearing down existing working directory " + localFile);
		
		return new DiskBackedMap.BucketBuffers<K, V>(localFile, maxEntriesInMemoryUnit);
	}

	public static <K, V extends SimpleMap.Keyable<K>> InMemorySimpleMap<K, V> getMemoryBackMap(Class<K> keyClass, Class<V> valueClass){
		
		return new InMemorySimpleMap<K, V>();
	}
}
