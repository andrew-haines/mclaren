package com.haines.mclaren.total_transations.util;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

//@NotThreadSafe
public class InMemorySimpleMap<K, V extends SimpleMap.Keyable<K>> implements SimpleMap<K, V>{
	
	private final Map<K, V> memoryMap = new HashMap<K, V>();
	
	@Override
	public V get(K key) {
		return memoryMap.get(key);
	}

	@Override
	public V put(K key, V value) {
		return memoryMap.put(key, value);
	}

	public void clear() {
		memoryMap.clear();
	}

	@Override
	public long size() {
		return memoryMap.size();
	}
	
	public Collection<V> values(){
		return memoryMap.values();
	}

	@Override
	public Stream<V> processAllEvents(Stream<V> events, BiFunction<V, V, V> reduceFunction) {
		return StreamSupport.stream(events.spliterator(), false)
				.map(e -> reduceFunction.apply(e, memoryMap.get(e.getKey())));
	}

	@Override
	public void close() throws IOException {
		// no op
	}

	@Override
	public Iterable<V> getAllValues() {
		return memoryMap.values();
	}
}