package com.haines.mclaren.total_transations.io;

import java.io.IOException;
import java.net.URI;

public interface IOFactory<E> {

	public Persister<E> createPersister(URI uri) throws IOException;
	
	public Feeder<E> createFeeder(URI uri) throws IOException;
}
