package com.haines.mclaren.total_transations.api;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.CoreMatchers.hasItems;
import java.io.IOException;
import java.util.Iterator;
import java.util.stream.StreamSupport;

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.junit.Before;
import org.junit.Test;

import com.haines.mclaren.total_transations.domain.UserEvent;
import com.haines.mclaren.total_transations.domain.UserEvent.ImmutableUserEvent;

public class TopNEventConsumerUnitTest {

	private TopNEventConsumer<UserEvent> candidate;
	
	@Before
	public void before(){
		candidate = new TopNEventConsumer<>(5, UserEvent.RANKED_BY_TRANSACTIONS);
	}
	
	@Test
	public void given1Event_whenCallingGetAndWait_thenTopNOrderedCorrectly() throws IOException, InterruptedException{
		candidate.consume(createTestEvent("haines123", 56));
		
		candidate.close();
		
		Iterable<UserEvent> best = candidate.waitAndGetBestNEvents();
		
		assertThat(best, hasItems(createTestEvent("haines123", 56)));
		assertThat(best, hasSize(1));
	}
	
	@Test
	public void given4Events_whenCallingGetAndWait_thenTopNOrderedCorrectly() throws IOException, InterruptedException{
		
		candidate.consume(createTestEvent("haines123", 56));
		candidate.consume(createTestEvent("haines124", 34));
		candidate.consume(createTestEvent("haines125", 87));
		candidate.consume(createTestEvent("haines126", 543));
		
		candidate.close();
		
		Iterable<UserEvent> best = candidate.waitAndGetBestNEvents();
		
		assertThat(best, hasAllItemsInOrder(createTestEvent("haines126", 543), 
											createTestEvent("haines125", 87),
											createTestEvent("haines123", 56),
											createTestEvent("haines124", 34)));
		assertThat(best, hasSize(4));
	}
	
	@Test
	public void given8Events_whenCallingGetAndWait_thenTopNOrderedCorrectly() throws IOException, InterruptedException{ // more than n so expect elements to be removed
		
		candidate.consume(createTestEvent("haines123", 345));
		candidate.consume(createTestEvent("haines124", 34));
		candidate.consume(createTestEvent("haines125", 45));
		candidate.consume(createTestEvent("haines126", 543));
		candidate.consume(createTestEvent("haines127", 65));
		candidate.consume(createTestEvent("haines128", 33));
		candidate.consume(createTestEvent("haines129", 65476));
		candidate.consume(createTestEvent("haines130", 4));
		
		candidate.close();
		
		Iterable<UserEvent> best = candidate.waitAndGetBestNEvents();
		
		assertThat(best, hasAllItemsInOrder(createTestEvent("haines129", 65476), 
											createTestEvent("haines126", 543),
											createTestEvent("haines123", 345),
											createTestEvent("haines127", 65),
											createTestEvent("haines125", 45)));
		assertThat(best, hasSize(5));
	}

	private UserEvent createTestEvent(String user, int numTransactions) {
		return new ImmutableUserEvent(user, numTransactions);
	}
	
	public Matcher<Iterable<?>> hasSize(int size){
		return new BaseMatcher<Iterable<?>>(){

			@Override
			public boolean matches(Object item) {
				Iterable<?> it = (Iterable<?>) item;
				return StreamSupport.stream(it.spliterator(), false).count() == size;
			}

			@Override
			public void describeTo(Description description) {
				description.appendValue("of size: "+size);
			}
			
		};
	}
	
	public <T> Matcher<Iterable<?>> hasAllItemsInOrder(@SuppressWarnings("unchecked") T... expectedItems){
		return new BaseMatcher<Iterable<?>>(){

			@Override
			public boolean matches(Object item) {
				Iterator<?> it = ((Iterable<?>) item).iterator();
				for (T expectedNextItem: expectedItems){
					if (it.hasNext()){
						if (!it.next().equals(expectedNextItem)){
							return false;
						}
					} else{
						return false;
					}
				}
				
				return !it.hasNext();
			}

			@Override
			public void describeTo(Description description) {
				description.appendValue("items"+ expectedItems);
			}
		};
	}
}
