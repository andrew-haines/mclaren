package com.haines.mclaren.total_transations.domain;

import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Stream;

import com.haines.mclaren.total_transations.api.Aggregator;
import com.haines.mclaren.total_transations.api.Aggregator.DirectStreamAggregatorProducer;
import com.haines.mclaren.total_transations.api.Consumer;
import com.haines.mclaren.total_transations.api.Consumer.ChainedConsumer;
import com.haines.mclaren.total_transations.api.Consumer.SeperateThreadConsumer;
import com.haines.mclaren.total_transations.util.CollectionUtil;
import com.haines.mclaren.total_transations.util.SimpleMap;
import com.haines.mclaren.total_transations.api.Dispatcher;
import com.haines.mclaren.total_transations.api.DomainFactory;
import com.haines.mclaren.total_transations.domain.UserEvent.MutableUserEvent;
import com.haines.mclaren.total_transations.io.Persister;

public class UserTransactionDomainFactory implements DomainFactory {
	
	private static final Logger LOG = Logger.getLogger(UserTransactionDomainFactory.class.getName());
	
	private final int numAggregatorWorkerThreads;
	private final int topN;
	private final Path diskOutput;
	private final int numInMemoryItemsPerExecutor;
	
	public UserTransactionDomainFactory(int numAggregatorWorkerThreads, int topN, Path diskOutput, int numInMemoryItemsPerExecutor){
		this.numAggregatorWorkerThreads = numAggregatorWorkerThreads;
		this.numInMemoryItemsPerExecutor = numInMemoryItemsPerExecutor;
		this.topN = topN;
		this.diskOutput = diskOutput;
	}
	
	@Override
	public Consumer<UserEvent> createInitalChainConsumer() throws IOException, ClassNotFoundException {
		try {
			return createInitalChainConsumer(getDefaultFinalConsumers());
		} catch (InterruptedException e) {
			throw new RuntimeException("unable to create consumer chain", e);
		}
	}
	
	public Consumer<UserEvent> getDefaultFinalConsumers() throws IOException{
		return ChainedConsumer.chain(createTopNConsumer(), createAggregationPersister());
	}

	public Consumer<UserEvent> createAggregationPersister() throws IOException {
		return Persister.FACTORY.createCSVPersister(getAggregationFile(diskOutput), UserEventSerializer.SERIALIZER);
	}

	public TopNEventConsumer<UserEvent> createTopNConsumer() {
		return new TopNEventConsumer<UserEvent>(topN, UserEvent.RANKED_BY_TRANSACTIONS);
	}

	public Consumer<UserEvent> createInitalChainConsumer(Consumer<UserEvent> finalPathConsumer) throws IOException, ClassNotFoundException, InterruptedException {
		
		int totalWorkerThreads = numAggregatorWorkerThreads + 1;
		SimpleMap<Serializable, MutableUserEvent> diskBackedStore = CollectionUtil.getFileBackedMap(createTmpMapDir(diskOutput), numInMemoryItemsPerExecutor);
		
		DirectStreamAggregatorProducer<UserEvent> finalAggregator = new DirectStreamAggregatorProducer<UserEvent>(diskBackedStore, finalPathConsumer); 
		
		CountDownLatch threadsStarted = new CountDownLatch(totalWorkerThreads);
		
		SeperateThreadConsumer<Stream<UserEvent>> finalAggregatorThread = new SeperateThreadConsumer<Stream<UserEvent>>(finalAggregator, threadsStarted);
		
		Collection<SeperateThreadConsumer<UserEvent>> concurrentConsumers = new ArrayList<SeperateThreadConsumer<UserEvent>>();
		
		for (int i = 0; i < numAggregatorWorkerThreads; i++){
			LOG.log(Level.INFO, "create new consumer thread: "+i);
			concurrentConsumers.add(new SeperateThreadConsumer<UserEvent>(new Aggregator.AggregatorWindowedProducer<UserEvent>(numInMemoryItemsPerExecutor, finalAggregatorThread), threadsStarted));
		}
		
		Executor executor = Executors.newFixedThreadPool(totalWorkerThreads, new ThreadFactory(){

			private int nextWorkerNumber = 0;
			
			@Override
			public Thread newThread(Runnable r) {
				return new Thread(r, "aggregation_worker_"+(nextWorkerNumber++));
			}
			
		}); // the extra thread is the final consumer
		
		Consumer<UserEvent> dispatcher = Dispatcher.FACTORY.createRoundRobinDispatch(concurrentConsumers);
		
		// submit all the threads
		concurrentConsumers.stream().forEach(e -> executor.execute(e));
		executor.execute(finalAggregatorThread);
		
		
		threadsStarted.await();
		return dispatcher;
	}

	private Path createTmpMapDir(Path diskOutput) {
		return Paths.get(diskOutput.toString(), "tmpMap");
	}
	
	private Path getAggregationFile(Path rootOutput) {
		return Paths.get(diskOutput.toString(), "out");
	}
}
