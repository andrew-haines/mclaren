package com.haines.mclaren.total_transations.domain;

import java.io.IOException;
import java.util.Comparator;
import java.util.TreeSet;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.haines.mclaren.total_transations.api.Consumer;
import com.haines.mclaren.total_transations.api.Event;

/**
 * A terminal consumer that computes the top n events based on the provided comparator. When the internal set exceeds the
 * number of the best events, the lowest ranked event is dropped.
 * @author haines
 *
 * @param <T>
 */
public class TopNEventConsumer<T extends Event<T>> implements Consumer<T>{

	private static final Logger LOG = Logger.getLogger(TopNEventConsumer.class.getName());
	
	private final TreeSet<T> bestAggregatedEvents;
	private final int n;
	
	public TopNEventConsumer(int n, Comparator<T> ordering){
		this.bestAggregatedEvents = new TreeSet<T>(ordering);
		this.n = n;
	}

	@Override
	public void close() throws IOException {
		LOG.log(Level.INFO, "calculated top {} events");
		
		int i = 0;
		for (T event: bestAggregatedEvents){
			LOG.log(Level.INFO, "{}. {} - {}", new Object[]{i++, event.getAggregationValue(), event.toString()});
		}
		
	}

	@Override
	public boolean consume(T e) {
		
		bestAggregatedEvents.add(e);
		
		if (bestAggregatedEvents.size() > n){
			bestAggregatedEvents.remove(bestAggregatedEvents.first()); // remove the lowest value.
		}
		return false;
	}
	
	public Iterable<T> getBestNEvents(){
		return bestAggregatedEvents;
	}
}
