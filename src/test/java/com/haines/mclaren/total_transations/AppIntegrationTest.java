package com.haines.mclaren.total_transations;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.junit.Before;
import org.junit.Test;

import com.haines.mclaren.total_transations.api.Consumer;
import com.haines.mclaren.total_transations.api.Consumer.ChainedConsumer;
import com.haines.mclaren.total_transations.domain.TopNEventConsumer;
import com.haines.mclaren.total_transations.domain.UserEvent;
import com.haines.mclaren.total_transations.domain.UserEvent.ImmutableUserEvent;
import com.haines.mclaren.total_transations.domain.UserTransactionDomainFactory;
import com.haines.mclaren.total_transations.io.Feeder;
import com.haines.mclaren.total_transations.io.Util;

/**
 * Integration test that first generates a large data stream that is bigger than 256mb and runs it through the app end to end.
 * 
 * The JVM is then restricted to run on 256MB, enforced by the maven test runner
 * @author haines
 *
 */
public class AppIntegrationTest {
	
	private static final int TEST_TOP_N = 5;

	private static final int TEST_NUM_ENTRIES_IN_AGGREGATION = 5000;

	private static final int TEST_EVENTS = 1_000_000;

	private static final long START_RND_SEED = 1343456464;

	private static final List<String> DEFAULT_LONG_TAIL = Arrays.asList("HAINES", "MCLAREN", "1234aBc", "AHaines", "Andy", "abC1234", "4321abc");

	
	/*
	 * with 5 max upper case digits it means a 1 / n!/(n-k)! = 24! / (24 - 5)! = 1/ 5100480 chance of a collision. 
	 * As we only create a million events there is an unlikely chance that this will generate a user to aggregate.
	 * Consequently i restrict my alphabet to the following 8 character alphabet (and hence a chance of getting a duplicate)
	 * user event being 1 / 6720
	 */
	private static final int TEST_MAX_USER_NAME_LENGTH = 5;
	
	private static final char[] SMALL_ALPHABET = new char[]{'a', 'h', 'i', 'n', 'e', 's', 'm', 'c'};
	
	/*
	 * The following uses an alphabet of 16 characters which makes the event of a collision much more unlikely. Excluding the
	 * long tail list, the chance is 1 / 524160 when choosing 5 character length user names
	 */
	
	private static final char[] LARGE_ALPHABET = new char[]{'a', 'h', 'i', 'n', 'e', 's', 'm', 'c', '1', '2', '3', '4', '5', '6', '7', '8'};

	private static final double TEST_LONG_TAIL_RATE = 0.1;
	
	private App candidate;
	private TopNEventConsumer<UserEvent> topNConsumer;
	private CountNumEventsConsumer countNumEventsConsumer;
	
	@Before
	public void before() throws ClassNotFoundException, IOException, URISyntaxException{
		
		UserTransactionDomainFactory domainFactory = new UserTransactionDomainFactory(Runtime.getRuntime().availableProcessors(), TEST_TOP_N, createTmpOutputDir(), TEST_NUM_ENTRIES_IN_AGGREGATION);
		
		this.countNumEventsConsumer = new CountNumEventsConsumer();
		this.topNConsumer = domainFactory.createTopNConsumer();
		
		Consumer<UserEvent> finalConsumer = ChainedConsumer.chain(topNConsumer, domainFactory.createAggregationPersister(), countNumEventsConsumer);
		
		domainFactory.createInitalChainConsumer(finalConsumer);
		
		candidate = new App(domainFactory.createInitalChainConsumer(finalConsumer));
	}
	
	public void after(){
		
	}
	
	@Test
	public void givenSyntheticFeederWithSmallAlphabet_whenCallingProcess_thenDataGeneratedAndAggregated() throws IOException{
		try(Feeder<UserEvent> feeder = new SyntheticFeeder(TEST_EVENTS, START_RND_SEED, DEFAULT_LONG_TAIL, TEST_LONG_TAIL_RATE, TEST_MAX_USER_NAME_LENGTH, SMALL_ALPHABET)){
			candidate.process(feeder);
		}
		
		candidate.close();
		
		Iterator<UserEvent> bestEvents = topNConsumer.getBestNEvents().iterator();
		assertEvent(bestEvents.next(), "", 123);
	}

	private void assertEvent(UserEvent event, String expectedUser, int numTransactions) {
		assertThat(event.getUser(), is(equalTo(expectedUser)));
		assertThat(event.getNumTransactions(), is(equalTo(numTransactions)));
	}

	private Path createTmpOutputDir() throws URISyntaxException, IOException {
		Path tmpPath = Paths.get(Paths.get(AppIntegrationTest.class.getResource("/").toURI()).toString(), "processed");
		
		Util.recursiveDelete(tmpPath);
		
		Files.createDirectory(tmpPath);
		
		return tmpPath;
	}
	
	private static class SyntheticFeeder implements Feeder<UserEvent>{

		private static final Logger LOG = Logger.getLogger(SyntheticFeeder.class.getName());
		
		private final int numEvents;
		private int currentEventNumber;
		private final Random rnd;
		private final double chanceOfPickingLongTail;
		private final List<String> longTailUserList;
		private final int maxUserNameLength;
		private final char[] alphabet;
		
		public SyntheticFeeder(int numEvents, long startSeed, List<String> longTailUserList, double chanceOfPickingLongTail, int maxUserNameLength, char[] alphabet) {
			this.numEvents = numEvents;
			this.rnd = new Random(startSeed);
			this.chanceOfPickingLongTail = chanceOfPickingLongTail;
			this.longTailUserList = longTailUserList;
			this.maxUserNameLength = maxUserNameLength;
			this.alphabet = alphabet;
		}

		@Override
		public boolean hasNext() {
			return currentEventNumber < numEvents;
		}

		@Override
		public UserEvent next() {
			try{
				UserEvent event = generateNextEvent();
				LOG.log(Level.INFO, "generated new event: "+event.toString());
				
				return event;
				
			}finally{
				currentEventNumber++;
			}
		}

		private UserEvent generateNextEvent() {
			return new ImmutableUserEvent(generateRandomUser(), generateRandomNumTransactions());
		}

		private String generateRandomUser() {
			
			// first see if we are using a user from the long tail
			
			double longTail = rnd.nextDouble();
			
			if (longTail < chanceOfPickingLongTail){
				return pickFromLongTail();
			} else{
				return generateRandomString();
			}
		}

		private String generateRandomString() {
			StringBuilder builder = new StringBuilder();
			
			for (int i = 0; i < maxUserNameLength; i++){
				builder.append(alphabet[Math.abs(rnd.nextInt() % alphabet.length)]);
			}
			
			return builder.toString();
		}

		private String pickFromLongTail() {
			return longTailUserList.get(Math.abs(rnd.nextInt() % longTailUserList.size()));
		}

		private long generateRandomNumTransactions() {
			return Math.abs(rnd.nextInt());
		}

		@Override
		public void close() throws IOException {
			// TODO Auto-generated method stub
			
		}
		
	}
	
	private static class CountNumEventsConsumer implements Consumer<UserEvent>{

		private final AtomicLong totalEvents;
		
		private CountNumEventsConsumer() {
			totalEvents = new AtomicLong();
		}
		
		@Override
		public void close() throws IOException {
			// no op
		}

		@Override
		public boolean consume(UserEvent event) {
			totalEvents.incrementAndGet();
			
			return true;
		}
		
	}
}
