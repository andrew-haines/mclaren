package com.haines.mclaren.total_transations.api;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;

import com.haines.mclaren.total_transations.util.CollectionUtil;

public class Dispatcher<E extends Event<E>> implements Closeable, Consumer<E>{

	public final static Factory FACTORY = new Factory();
	
	private final Iterator<? extends Consumer<E>> consumers;
	
	private Dispatcher(Iterable<? extends Consumer<E>> consumers){
		this.consumers = consumers.iterator();
	}
	
	public void dispatchEvent(E event){
		while(!consumers.next().consume(event)); // busy spins until a consumer is able to take this event. TODO this should really be better
	}
	
	public static class Factory {
		
		private Factory(){}
		
		public <E extends Event<E>> Dispatcher<E> createRoundRobinDispatch(Iterable<? extends Consumer<E>> consumers){
			return new Dispatcher<E>(CollectionUtil.cycle(consumers)); // make this loop forever to simulate a round robin.
		}
		
		// we can add other dispachers here such as ones that look at the capacity of each queue etc.
	}

	@Override
	public void close() throws IOException {
		
	}

	@Override
	public boolean consume(E e) {
		dispatchEvent(e);
		
		return true; // always returns true as the above will always complete unless there is an unchecked exception
	}
}
