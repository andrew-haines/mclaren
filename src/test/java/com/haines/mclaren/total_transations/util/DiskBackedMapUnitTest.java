package com.haines.mclaren.total_transations.util;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import java.io.IOException;
import java.io.Serializable;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import com.haines.mclaren.total_transations.SyntheticFeeder;
import com.haines.mclaren.total_transations.domain.UserEvent;
import com.haines.mclaren.total_transations.domain.UserEvent.MutableUserEvent;
import com.haines.mclaren.total_transations.domain.UserTransactionDomainFactory;
import com.haines.mclaren.total_transations.io.Feeder;
import com.haines.mclaren.total_transations.io.Util;
import com.haines.mclaren.total_transations.util.DiskBackedMap.BucketBuffers;
import com.haines.mclaren.total_transations.util.DiskBackedMap.BucketBuffers.Node;

public class DiskBackedMapUnitTest {

	private static final long TEST_SEED = 9456723456l;
	private DiskBackedMap<Serializable, MutableUserEvent> candidate;
	
	@Before
	public void before() throws ClassNotFoundException, IOException, URISyntaxException{
		candidate = CollectionUtil.getFileBackedMap(getTmpLocation(), 5, UserTransactionDomainFactory.createIOFactory(6144));
	}

	private Path getTmpLocation() throws URISyntaxException, IOException {
		Path tmpPath = Paths.get(Paths.get(DiskBackedMapUnitTest.class.getResource("/").toURI()).toString(), "diskBackedMapTest");
		
		Util.recursiveDelete(tmpPath);
		
		Files.createDirectory(tmpPath);
		
		return tmpPath;
	}
	
	@Test
	public void givenKey_whenCallingGetBucketOnNodeWith0Depth_thenSingleLevelHashingPerformed() throws URISyntaxException, IOException{
		Node testNode = new Node(0, 11, getTmpLocation(), 0);
		
		assertThat(BucketBuffers.getBucketNumForKey(testNode, 1234), is(equalTo(0))); // just the node itself as it is a leaf
	}
	
	@Test
	public void givenKey_whenCallingGetBucketOnNodeWith1Depth_thenSingleLevelHashingPerformed() throws IOException, URISyntaxException{
		Node testNode = new Node(0, 11, getTmpLocation(), 0);
		testNode.promoteToBranchNode(0);
		
		assertThat(BucketBuffers.getBucketNumForKey(testNode, 1234), is(equalTo(Math.abs(1234 % 11) + 1))); // now this is a mod of the hashcode and the number of children it has (1 based due to each child id starting from +1 of the parent)
	}
	
	@Test
	public void givenKey_whenCallingGetBucketOnNodeWith2Depth_thenSingleLevelHashingPerformed() throws IOException, URISyntaxException{
		Node testNode = new Node(0, 11, getTmpLocation(), 0);
		int maxId = testNode.promoteToBranchNode(0);
		
		// now promote node at bucket 2 so this has to hash twice
		maxId = testNode.getChild(2).promoteToBranchNode(maxId);
		
		// all these hashes hash to the same bucket under the root parent (child num 2). The following tests that their hash codes are manipulated 
		// successfully to distribute them amoungst that child bucket
		
		assertThat(BucketBuffers.getBucketNumForKey(testNode, 13), is(equalTo(18)));
		assertThat(BucketBuffers.getBucketNumForKey(testNode, 29), is(equalTo(15)));
		assertThat(BucketBuffers.getBucketNumForKey(testNode, 1234), is(equalTo(21)));
		//assertThat(BucketBuffers.getBucketNumForKey(testNode, 2464), is(equalTo(11)));
		
		assertThat(BucketBuffers.getBucketNumForKey(testNode, 18), is(equalTo(21)));
		assertThat(BucketBuffers.getBucketNumForKey(testNode, 34), is(equalTo(13)));
	}
	
	@Test
	public void given4Events_whenCallingPut_thenEventsStoredSuccessfully(){
		for (MutableUserEvent event: getRandomEvents(TEST_SEED, 4)){
			candidate.put(event.getAggregationValue(), event);
		}
		
		assertThat(candidate.size(), is(equalTo(4l)));
		
		for (MutableUserEvent event: getRandomEvents(TEST_SEED, 4)){ // seed is the same so therefore contents are also
			assertThat(candidate.get(event.getKey()), is(equalTo(event)));
		}
	}
	
	@Test
	public void given6Events_whenCallingPut_thenEventsStoredSuccessfully(){ // checks that it spill bucket into 
		for (MutableUserEvent event: getRandomEvents(TEST_SEED, 6)){
			candidate.put(event.getAggregationValue(), event);
		}
		
		assertThat(candidate.size(), is(equalTo(6l)));
		
		for (UserEvent event: getRandomEvents(TEST_SEED, 6)){ // seed is the same so therefore contents are also
			assertThat(candidate.get(event.getKey()), is(equalTo(event)));
		}
	}
	
	@Test
	public void given6Events_whenCallingPutAndGetAll_thenEventsReturnedSuccessfully(){ // checks that it spill bucket into 
		addElementsAndCheckContentsOfMap(getRandomEvents(TEST_SEED, 6));
	}
	
	@Test
	public void given24Events_whenCallingPutAndGetAll_thenEventsReturnedSuccessfully(){ // last set of events of this seed where the buckets only go to a depth of 1
		addElementsAndCheckContentsOfMap(getRandomEvents(TEST_SEED, 24));
	}
	
	@Test
	public void given25Events_whenCallingPutAndGetAll_thenEventsReturnedSuccessfully(){ // with this seed, this is the first time the buckets go to depth 2
		addElementsAndCheckContentsOfMap(getRandomEvents(TEST_SEED, 25));
	}
	
	@Test
	public void given2500Events_whenCallingPutAndGetAll_thenEventsReturnedSuccessfully(){ // put a larger amount of data through it to generate a big tree
		addElementsAndCheckContentsOfMap(getRandomEvents(TEST_SEED, 2500));
	}
	
	private void addElementsAndCheckContentsOfMap(Iterable<MutableUserEvent> events){
		
		long numEvents = 0;
		Map<String, UserEvent> duplicateUsers = new HashMap<String, UserEvent>(); // the nature of the synthetic generator means there maybe duplicate events of the same user. Build our dup map here
		for (MutableUserEvent event: events){
			numEvents++;
			System.out.println("Adding event: "+event);
			MutableUserEvent previousElement = candidate.put(event.getAggregationValue(), event);
			
			if (previousElement != null){
				System.out.println("Discovered duplicate event: "+previousElement+" adding latest entry "+event+" to dup map");
				duplicateUsers.put(previousElement.getUser(), event);
			}
			
		}
		
		Map<Serializable, MutableUserEvent> allEvents = CollectionUtil.loadAllElementsIntoMemoryMap(candidate);
		
		for (UserEvent event: events){ // seed is the same so therefore contents are also
			
			UserEvent previousElement = duplicateUsers.get(event.getUser());
			if (previousElement != null) {
				// if we have this user in our dup key list, assert that it is this one that we have stored (this will have overriden the value)
				event = previousElement;
			}
			assertThat(allEvents.get(event.getKey()), is(equalTo(event)));
		}
		
		numEvents -= duplicateUsers.size(); // all the dups will be at least 1 less (multiple collisions will be even more so this will need to change if that ever happens on future tests that use this method)
		
		assertThat(allEvents.size(), is(equalTo((int)numEvents)));
		assertThat(candidate.size(), is(equalTo(numEvents)));
	}

	private Iterable<MutableUserEvent> getRandomEvents(long testSeed, int numEvents) {
		return new Iterable<MutableUserEvent>(){

			@Override
			public Iterator<MutableUserEvent> iterator() {
				@SuppressWarnings("resource") // synthetic feeder does not need closing
				Feeder<UserEvent> delegate = new SyntheticFeeder(numEvents, testSeed, Collections.emptyList(), 0.0, 5, SyntheticFeeder.LARGE_ALPHABET);
				
				return new Iterator<MutableUserEvent>(){

					@Override
					public boolean hasNext() {
						return delegate.hasNext();
					}

					@Override
					public MutableUserEvent next() {
						return (MutableUserEvent)delegate.next().toMutableEvent();
					}
				};
			}
			
		};
	}
}
