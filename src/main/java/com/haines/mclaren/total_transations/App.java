package com.haines.mclaren.total_transations;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

import com.haines.mclaren.total_transations.api.Consumer;
import com.haines.mclaren.total_transations.api.DomainFactory;
import com.haines.mclaren.total_transations.domain.UserEvent;
import com.haines.mclaren.total_transations.domain.UserEvent.ImmutableUserEvent;
import com.haines.mclaren.total_transations.domain.UserEventDeserializer;
import com.haines.mclaren.total_transations.domain.UserTransactionDomainFactory;
import com.haines.mclaren.total_transations.io.Feeder;

/**
 * Main entry to application
 * @author haines
 *
 */
public class App implements Closeable{
	
	private final Consumer<UserEvent> consumer;
	
	public App(Consumer<UserEvent> consumer){
		this.consumer = consumer;
	}
	
	public void process(Feeder<? extends UserEvent> feeder) throws IOException{
		
		while(feeder.hasNext()){
			consumer.consume(feeder.next());
		}
	}
	
	@Override
	public void close() throws IOException {
		consumer.close();
	}
	
	public static App createApp(DomainFactory domainFactory) throws IOException, ClassNotFoundException{
		
		return new App(domainFactory.createInitalChainConsumer());
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException{
		
		if (args.length != 4){
			throw new IllegalArgumentException("USUAGE: java "+App.class.getName()+" {numAggregatorWorkerThreads} {topN} {numInMemoryItemsPerExecutor} {diskInput} {diskOutput}");
		}
		
		int numAggregatorWorkerThreads = Integer.parseInt(args[0]);
		int topN = Integer.parseInt(args[1]);
		int numInMemoryItemsPerExecutor = Integer.parseInt(args[2]);
		
		Path diskInput = Paths.get(args[3]);
		Path diskOutput = Paths.get(args[4]);
		
		try(App app = createApp(new UserTransactionDomainFactory(numAggregatorWorkerThreads, topN, diskOutput, numInMemoryItemsPerExecutor))){
			
			try(Feeder<ImmutableUserEvent> feeder = Feeder.FACTORY.createFileFeeder(diskInput, UserEventDeserializer.IMMUTABLE_DESERIALIZER, true)){
				app.process(feeder);
			}
		}
	}
}
