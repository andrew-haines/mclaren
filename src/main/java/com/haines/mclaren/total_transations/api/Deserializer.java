package com.haines.mclaren.total_transations.api;

import java.nio.ByteBuffer;

public interface Deserializer<E extends Event<E>> {

	E deserialise(ByteBuffer buffer, char fieldDelimiter, char eventDelimiter);

}
