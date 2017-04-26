package com.haines.mclaren.total_transations.api;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;

import com.haines.mclaren.total_transations.util.CollectionUtil;

public class Dispatcher<E extends Event<E>> implements Closeable, Consumer<E>{

	public final static Factory FACTORY = new Factory();
	
	private final Iterator<? extends Consumer<E>> consumerIt;
	private final Collection<? extends Consumer<E>> allConsumers;
	
	private Dispatcher(Iterable<? extends Consumer<E>> consumerIt, Collection<? extends Consumer<E>> allConsumers){
		this.consumerIt = consumerIt.iterator();
		this.allConsumers = allConsumers;
	}
	
	public void dispatchEvent(E event){
		while(!consumerIt.next().consume(event)); // busy spins until a consumer is able to take this event. TODO this should really be better
	}
	
	public static class Factory {
		
		private Factory(){}
		
		public <E extends Event<E>> Dispatcher<E> createRoundRobinDispatch(Collection<? extends Consumer<E>> consumers){
			return new Dispatcher<E>(CollectionUtil.cycle(consumers), consumers); // make this loop forever to simulate a round robin.
		}
		
		// we can add other dispachers here such as ones that look at the capacity of each queue etc.
	}

	@Override
	public void close() throws IOException {
		allConsumers.forEach(e -> {
			try {
				e.close();
			} catch (IOException ex) {
				throw new RuntimeException("Unable to close consumer: "+e, ex);
			}
		});
	}

	@Override
	public boolean consume(E e) {
		dispatchEvent(e);
		
		return true; // always returns true as the above will always complete unless there is an unchecked exception
	}
}
