package com.haines.mclaren.total_transations.io;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.junit.Before;
import org.junit.Test;

import com.haines.mclaren.total_transations.domain.UserEvent.ImmutableUserEvent;
import com.haines.mclaren.total_transations.domain.UserEventDeserializer;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class FeederUnitTest {

	private Feeder<ImmutableUserEvent> candidate;
	
	@Before
	public void before() throws IOException, URISyntaxException{
		candidate = Feeder.FACTORY.createFileFeeder(loadTestFile("/userEvents_small.txt"), UserEventDeserializer.IMMUTABLE_DESERIALIZER, false, 1024);
	}
	
	@Test
	public void givenEventsFile_whenIterating_allEventsReturned(){
		assertThat(candidate.hasNext(), is(equalTo(true)));
		assertEvent(candidate.next(), "5i1a5",1723019229);
		assertThat(candidate.hasNext(), is(equalTo(true)));
		assertEvent(candidate.next(), "2ehsc",1769713922);
		assertThat(candidate.hasNext(), is(equalTo(true)));
		assertEvent(candidate.next(), "7hcie",22409236);
		assertThat(candidate.hasNext(), is(equalTo(true)));
		assertEvent(candidate.next(), "nc71n",2055697719);
		assertThat(candidate.hasNext(), is(equalTo(true)));
		assertEvent(candidate.next(), "574i6",435205670);
		assertThat(candidate.hasNext(), is(equalTo(false)));
	}
	
	@Test
	public void givenEventsFile_whenIteratingUsingMemoryMapping_allEventsReturned() throws IOException, URISyntaxException{
		
		Feeder<ImmutableUserEvent> candidate = Feeder.FACTORY.createFileFeeder(loadTestFile("/userEvents_small.txt"), UserEventDeserializer.IMMUTABLE_DESERIALIZER, true, 64);
		
		assertThat(candidate.hasNext(), is(equalTo(true)));
		assertEvent(candidate.next(), "5i1a5",1723019229);
		assertThat(candidate.hasNext(), is(equalTo(true)));
		assertEvent(candidate.next(), "2ehsc",1769713922);
		assertThat(candidate.hasNext(), is(equalTo(true)));
		assertEvent(candidate.next(), "7hcie",22409236);
		assertThat(candidate.hasNext(), is(equalTo(true)));
		assertEvent(candidate.next(), "nc71n",2055697719);
		assertThat(candidate.hasNext(), is(equalTo(true)));
		assertEvent(candidate.next(), "574i6",435205670);
		assertThat(candidate.hasNext(), is(equalTo(false)));
	}

	private void assertEvent(ImmutableUserEvent event, String expectedUser, long expectedNumTransactions) {
		assertThat(event.getUser(), is(equalTo(expectedUser)));
		assertThat(event.getNumTransactions(), is(equalTo(expectedNumTransactions)));
	}

	private Path loadTestFile(String resourceName) throws URISyntaxException {
		return Paths.get(FeederUnitTest.class.getResource(resourceName).toURI());
	}
}
