package com.haines.mclaren.total_transations.util;

import java.io.Closeable;
import java.io.Serializable;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.stream.Stream;

/**
 * A much simpler interface for {@link Map} to avoid unneccessary code or use of {@link UnsupportedOperationException}
 * when writing the file mapped implementation.
 */
public interface SimpleMap<K, V extends SimpleMap.Keyable<K>> extends Closeable{
	
	V get(K key);
	
	V put(K key, V value);
	
	/**
	 * The following will call the following reduce function on all the events with corresponding events, if applicable,
	 * to events already stored under the same key.
	 * 
	 * @param events
	 * @param reduceFunction
	 */
	Stream<V> processAllEvents(Stream<V> events, BiFunction<V, V, V> reduceFunction);
	
	/**
	 * puts all elements into this map. Not that in order to work, something has to invoke one of the iteration methods
	 * of {@link Stream}
	 * @param events
	 * @return A stream mapping containing any existing entries or null if there was no corresponding entry
	 */
	default Stream<V> putAllEvents(Stream<V> events){
		return events.map(e -> put(e.getKey(), e));
	}

	/**
	 * Returns the total number of elements in this map
	 * @return
	 */
	long size();
	
	/**
	 * Returns all the values stored in this map.
	 * @return
	 */
	public Iterable<V> getAllValues();
	
	public static interface Keyable<K> extends Serializable {
		K getKey();
	}
}