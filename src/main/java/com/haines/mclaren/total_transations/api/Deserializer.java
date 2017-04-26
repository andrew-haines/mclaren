package com.haines.mclaren.total_transations.api;

import java.nio.ByteBuffer;

public interface Deserializer<E> {

	E deserialise(ByteBuffer buffer, char fieldDelimiter, char eventDelimiter);

}
