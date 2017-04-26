package com.haines.mclaren.total_transations.domain;

import java.io.IOException;
import java.util.Comparator;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
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
	private final CountDownLatch finished;
	
	public TopNEventConsumer(int n, Comparator<T> ordering){
		this.bestAggregatedEvents = new TreeSet<T>(ordering);
		this.n = n;
		this.finished = new CountDownLatch(1);
	}

	@Override
	public void close() throws IOException {
		LOG.log(Level.INFO, "calculated top {} events");
		
		int i = 0;
		for (T event: bestAggregatedEvents){
			LOG.log(Level.INFO, "{}. {} - {}", new Object[]{i++, event.getAggregationValue(), event.toString()});
		}
		finished.countDown();
	}

	@Override
	public boolean consume(T e) {
		
		bestAggregatedEvents.add(e);
		
		if (bestAggregatedEvents.size() > n){
			bestAggregatedEvents.remove(bestAggregatedEvents.first()); // remove the lowest value.
		}
		return false;
	}
	
	/**
	 * Blocks and waits until this is finished and returns the best n events.
	 * @return
	 * @throws InterruptedException
	 */
	public Iterable<T> waitAndGetBestNEvents() throws InterruptedException{
		finished.await();
		return bestAggregatedEvents;
	}
}
