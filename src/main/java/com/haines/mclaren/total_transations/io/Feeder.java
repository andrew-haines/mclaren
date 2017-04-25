package com.haines.mclaren.total_transations.io;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;

import com.haines.mclaren.total_transations.api.Deserializer;
import com.haines.mclaren.total_transations.api.Event;

public interface Feeder<E extends Event<E>> extends Iterator<E>, Closeable{

	public static final char CSV_FIELD_DELIMITER = ',';
	public static final char CSV_EVENT_DELIMITER = ',';
	
	public static final Factory FACTORY = new Factory();
	
	public static class Factory {
		
		private Factory(){}
		
		public <E extends Event<E>> Feeder<E> createFileFeeder(Path localFile, char fieldDelimier, char eventDelimiter, Deserializer<E> deserializer) throws IOException{
			
			FileChannel channel = FileChannel.open(localFile, StandardOpenOption.READ);
			
			ByteBuffer buffer = channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size());
			
			return new ByteBufferFeeder<E>(channel, buffer, fieldDelimier, eventDelimiter, deserializer);
		}
		
		public <E extends Event<E>> Feeder<E> createFileFeeder(Path localFile, Deserializer<E> deserializer) throws IOException{
			
			return createFileFeeder(localFile, CSV_FIELD_DELIMITER, CSV_EVENT_DELIMITER, deserializer);
		}
		
		/**
		 * A byte buffer based feeder used rather than the more simple {@link java.nio.file.Files#lines(Path)} utility
		 * because this will use direct memory access avoiding the copy into the heap (and the java object creation overhead) 
		 * for the 
		 * @author haines
		 *
		 */
		private static class ByteBufferFeeder<E extends Event<E>> implements Feeder<E> {

			private final ByteBuffer buffer;
			private final Closeable channel;
			private final char fieldDelimiter;
			private final char eventDelimiter;
			private final Deserializer<E> deserializer;
			
			private ByteBufferFeeder(Closeable channel, ByteBuffer buffer, char fieldDelimiter, char eventDelimiter, Deserializer<E> deserializer){
				this.channel = channel;
				this.buffer = buffer;
				this.fieldDelimiter = fieldDelimiter;
				this.eventDelimiter = eventDelimiter;
				this.deserializer = deserializer;
			}
			
			public boolean hasNext() {
				return buffer.hasRemaining();
			}

			public E next() {
				
				return deserializer.deserialise(buffer, fieldDelimiter, eventDelimiter);
			}

			@Override
			public void close() throws IOException {
				channel.close();
			}
		}
	}
}
