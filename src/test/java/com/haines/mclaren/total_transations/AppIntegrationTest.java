package com.haines.mclaren.total_transations;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Before;
import org.junit.Test;

import com.haines.mclaren.total_transations.api.Consumer;
import com.haines.mclaren.total_transations.api.Consumer.ChainedConsumer;
import com.haines.mclaren.total_transations.api.TopNEventConsumer;
import com.haines.mclaren.total_transations.domain.UserEvent;
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

	private static final int TEST_NUM_ENTRIES_IN_AGGREGATION = 500;

	private static final int TEST_EVENTS = 1_000;

	private static final long START_RND_SEED = 1343456464;

	private static final double TEST_LONG_TAIL_RATE = 0.1;
	
	private App candidate;
	private TopNEventConsumer<UserEvent> topNConsumer;
	private CountNumEventsConsumer countNumEventsConsumer;
	
	@Before
	public void before() throws ClassNotFoundException, IOException, URISyntaxException, InterruptedException{
		
		UserTransactionDomainFactory domainFactory = new UserTransactionDomainFactory(Runtime.getRuntime().availableProcessors(), TEST_TOP_N, createTmpOutputDir(), TEST_NUM_ENTRIES_IN_AGGREGATION);
		
		this.countNumEventsConsumer = new CountNumEventsConsumer();
		this.topNConsumer = domainFactory.createTopNConsumer();
		
		Consumer<UserEvent> finalConsumer = ChainedConsumer.chain(topNConsumer, domainFactory.createAggregationPersister(), countNumEventsConsumer);
		
		candidate = new App(domainFactory.createInitalChainConsumer(finalConsumer));
	}
	
	@Test
	public void givenSyntheticFeederWithSmallAlphabet_whenCallingProcess_thenDataGeneratedAndAggregated() throws IOException, InterruptedException{
		try(Feeder<UserEvent> feeder = new SyntheticFeeder(TEST_EVENTS, START_RND_SEED, SyntheticFeeder.DEFAULT_LONG_TAIL, TEST_LONG_TAIL_RATE, SyntheticFeeder.TEST_MAX_USER_NAME_LENGTH, SyntheticFeeder.SMALL_ALPHABET)){
			candidate.process(feeder);
		}
		
		candidate.close();
		
		Iterator<UserEvent> bestEvents = topNConsumer.waitAndGetBestNEvents().iterator();
		//assertEvent(bestEvents.next(), "eneie", 123);
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
