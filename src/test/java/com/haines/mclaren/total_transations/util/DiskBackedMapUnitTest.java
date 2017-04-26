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
import java.util.Iterator;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import com.haines.mclaren.total_transations.SyntheticFeeder;
import com.haines.mclaren.total_transations.domain.UserEvent;
import com.haines.mclaren.total_transations.io.Util;

public class DiskBackedMapUnitTest {

	private static final long TEST_SEED = 9456723456l;
	private DiskBackedMap<Serializable, UserEvent> candidate;
	
	@Before
	public void before() throws ClassNotFoundException, IOException, URISyntaxException{
		candidate = CollectionUtil.getFileBackedMap(getTmpLocation(), 5);
	}

	private Path getTmpLocation() throws URISyntaxException, IOException {
		Path tmpPath = Paths.get(Paths.get(DiskBackedMapUnitTest.class.getResource("/").toURI()).toString(), "diskBackedMapTest");
		
		Util.recursiveDelete(tmpPath);
		
		Files.createDirectory(tmpPath);
		
		return tmpPath;
	}
	
	@Test
	public void given4Events_whenCallingPut_thenEventsStoredSuccessfully(){
		for (UserEvent event: getRandomEvents(TEST_SEED, 4)){
			candidate.put(event.getAggregationValue(), event);
		}
		
		assertThat(candidate.size(), is(equalTo(4l)));
		
		for (UserEvent event: getRandomEvents(TEST_SEED, 4)){ // seed is the same so therefore contents are also
			assertThat(candidate.get(event.getKey()), is(equalTo(event)));
		}
	}
	
	@Test
	public void given6Events_whenCallingPut_thenEventsStoredSuccessfully(){ // checks that it spill bucket into 
		for (UserEvent event: getRandomEvents(TEST_SEED, 6)){
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
	
	private void addElementsAndCheckContentsOfMap(Iterable<UserEvent> events){
		
		long numEvents = 0;
		for (UserEvent event: events){
			numEvents++;
			System.out.println("Adding event: "+event);
			candidate.put(event.getAggregationValue(), event);
		}
		
		assertThat(candidate.size(), is(equalTo(numEvents)));
		
		Map<Serializable, UserEvent> allEvents = CollectionUtil.loadAllElementsIntoMemoryMap(candidate);
		
		for (UserEvent event: events){ // seed is the same so therefore contents are also
			assertThat(allEvents.get(event.getKey()), is(equalTo(event)));
		}
		
		assertThat(allEvents.size(), is(equalTo((int)numEvents)));
	}

	private Iterable<UserEvent> getRandomEvents(long testSeed, int numEvents) {
		return new Iterable<UserEvent>(){

			@Override
			public Iterator<UserEvent> iterator() {
				return new SyntheticFeeder(numEvents, testSeed, Collections.emptyList(), 0.0, 5, SyntheticFeeder.LARGE_ALPHABET);
			}
			
		};
	}
}
