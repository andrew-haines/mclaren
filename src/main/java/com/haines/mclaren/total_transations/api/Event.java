package com.haines.mclaren.total_transations.api;

import java.io.Serializable;

import com.haines.mclaren.total_transations.util.SimpleMap.Keyable;

public interface Event<E extends Event<E>> extends Keyable<Serializable>, Serializable{

	Serializable getAggregationValue();
	
	default Serializable getKey(){
		return getAggregationValue();
	}
	
	E toImmutableEvent();
	
	/**
	 * Due to the single writer pradigm and the non use of volatile on numTransactions (to keep things cache friendly),
	 * The compromise is we have to create memory visible versions. This provides a way of the writer thread to create
	 * immutable instances that can be visible to other threads.
	 * @return
	 */
	MutableEvent<E> toMutableEvent();
	
	public static interface MutableEvent<E extends Event<E>> extends Event<E>{
		
		void aggregate(E event);
	}
}