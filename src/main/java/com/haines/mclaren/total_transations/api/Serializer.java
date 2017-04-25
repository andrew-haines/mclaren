package com.haines.mclaren.total_transations.api;

import java.nio.ByteBuffer;

public interface Serializer<E extends Event<E>> {

	void serialise(E event, ByteBuffer buffer, char fieldDelimiter, char eventDelimiter);
}
