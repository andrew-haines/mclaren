package com.haines.mclaren.total_transations.api;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.util.function.BiFunction;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.haines.mclaren.total_transations.api.Event.MutableEvent;
import com.haines.mclaren.total_transations.util.CollectionUtil;
import com.haines.mclaren.total_transations.util.InMemorySimpleMap;
import com.haines.mclaren.total_transations.util.SimpleMap;

public abstract class Aggregator<E extends Event<E>> implements Consumer<E>, Closeable{
	
	private final static Logger LOG = Logger.getLogger(Aggregator.class.getName());

	private static final BiFunction<? extends MutableEvent<?>, ? extends MutableEvent<?>, ? extends MutableEvent<?>> DEFAULT_EVENT_AGGREGATOR = new BiFunction<MutableEvent<?>, MutableEvent<?>, MutableEvent<?>>(){

		@SuppressWarnings({ "rawtypes", "unchecked" })
		@Override
		public MutableEvent apply(MutableEvent t, MutableEvent u) {
			if (u != null){
				t.aggregate(u);
			}
			
			return t;
		}
	};
	
	private final SimpleMap<Serializable, MutableEvent<E>> aggregationBuffer;
	private final int windowSize;
	private final BiFunction<MutableEvent<E>, MutableEvent<E>, MutableEvent<E>> reduceFunction;
	
	@SuppressWarnings("unchecked")
	public Aggregator(int windowSize, SimpleMap<? extends Serializable, ? extends MutableEvent<E>> aggregationBuffer, BiFunction<MutableEvent<E>, MutableEvent<E>, MutableEvent<E>> reduceFunction){
		this.aggregationBuffer = (SimpleMap<Serializable, MutableEvent<E>>)aggregationBuffer;
		this.windowSize = windowSize;
		this.reduceFunction = reduceFunction;
	}
	
	@SuppressWarnings("unchecked")
	public Aggregator(int windowSize, SimpleMap<? extends Serializable, ? extends MutableEvent<E>> aggregationBuffer){
		this(windowSize, aggregationBuffer, (BiFunction<MutableEvent<E>, MutableEvent<E>, MutableEvent<E>>)DEFAULT_EVENT_AGGREGATOR);
	}
	
	public boolean consume(E event) {
		MutableEvent<E> existingEvent = aggregationBuffer.get(event.getAggregationValue());
		
		if (existingEvent != null){
			existingEvent = reduceFunction.apply(event.toMutableEvent(), existingEvent);
		} else{
			aggregationBuffer.put(event.getAggregationValue(), event.toMutableEvent()); // upgrade to mutable copy as this thread now owns it.
		}
		
		checkCapacityAndPush(event);
		
		return true;
	}

	private void checkCapacityAndPush(E event) {
		if (aggregationBuffer.size() > windowSize && windowSize != -1){
			LOG.log(Level.INFO, "aggregation buffer is full at "+ aggregationBuffer.size()+" items. pushing downstream");
			
			pushBufferDownStream(event);
		}
	}
	
	protected void pushBufferDownStream(E event) {
		// no op
	}

	@Override
	public void close() throws IOException {
		pushBufferDownStream(null);
	}
	
	public static class DirectStreamAggregatorProducer<E extends Event<E>> implements Consumer<Stream<E>>{
		private final static Logger LOG = Logger.getLogger(DirectStreamAggregatorProducer.class.getName());
		
		private final Consumer<E> downStreamConsumer;
		private final SimpleMap<? extends Serializable, MutableEvent<E>> aggregationBuffer;
		
		@SuppressWarnings("unchecked")
		public DirectStreamAggregatorProducer(SimpleMap<? extends Serializable, ? extends MutableEvent<E>> aggregationBuffer, Consumer<E> downStreamConsumer){
			
			this.aggregationBuffer = (SimpleMap<? extends Serializable, MutableEvent<E>>)aggregationBuffer;
			this.downStreamConsumer = downStreamConsumer;
		}

		@Override
		public void close() throws IOException {
			
			aggregationBuffer.close();
			
			// now this is aggregated, push all the aggregations downstream
			
			StreamSupport.stream(aggregationBuffer.getAllValues().spliterator(), false)
							.map(e -> e.toImmutableEvent()) // convert to immutable version for visibility
							.forEach(e -> downStreamConsumer.consume(e));
		}

		@SuppressWarnings("unchecked")
		@Override
		public boolean consume(Stream<E> events) {
			Stream<MutableEvent<E>> mappedStream = aggregationBuffer.processAllEvents(events.map(e -> e.toMutableEvent()), (BiFunction<MutableEvent<E>, MutableEvent<E>, MutableEvent<E>>)DEFAULT_EVENT_AGGREGATOR);
			
			/* Do a reduce to kick off the iteration of the stream. we map first to a 2 element array where the first element is the
			*  number of newly inserted elements and the second number is the number of aggregated elements (elements where we had an
			*  existing entry)
			*/
			int[] updateCounts = aggregationBuffer.putAllEvents(mappedStream).map(e -> e == null?new int[]{1,0}:new int[]{0,1}).reduce((e1, e2) -> new int[]{e1[0] + e2[0], e1[1] + e2[1]}).get(); 
			
			LOG.log(Level.FINE, "Out of "+updateCounts[0]+updateCounts[1]+" total events, "+updateCounts[0]+" were new and "+updateCounts[1]+" were aggregated");
			return true;
		}
	}

	public static class AggregatorWindowedProducer<E extends Event<E>> extends Aggregator<E>{

		private final static Logger LOG = Logger.getLogger(AggregatorWindowedProducer.class.getName());
		
		private final Consumer<Stream<E>> downStreamConsumer;
		private final InMemorySimpleMap<Serializable, ? extends MutableEvent<E>> aggregationBuffer;
		
		@SuppressWarnings("unchecked")
		public AggregatorWindowedProducer(int windowSize, InMemorySimpleMap<Serializable, ? extends MutableEvent<E>> aggregationBuffer, Consumer<? extends Stream<E>> downStreamConsumer) {
			super(windowSize, aggregationBuffer);
			
			this.aggregationBuffer = aggregationBuffer;
			this.downStreamConsumer = (Consumer<Stream<E>>)downStreamConsumer;
		}
		
		@SuppressWarnings("unchecked")
		public AggregatorWindowedProducer(int windowSize, Consumer<? extends Stream<E>> downStreamConsumer){
			this(windowSize, (InMemorySimpleMap<Serializable, ? extends MutableEvent<E>>)CollectionUtil.getMemoryBackMap(Serializable.class, MutableEvent.class), downStreamConsumer);
		}
		
		@Override
		protected void pushBufferDownStream(E event) {
			LOG.log(Level.INFO, "pushing buffer of "+aggregationBuffer.size()+" entries downstream");
			downStreamConsumer.consume(aggregationBuffer.values().stream()
			  	.map(e -> e.toImmutableEvent()));// map to immutable version to ensure memory visibility as we will be handing off to another thread.

			aggregationBuffer.clear(); // reset buffer
		}
	}
}
