package com.haines.mclaren.total_transations.io;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;

import com.haines.mclaren.total_transations.api.Deserializer;

public interface Feeder<E> extends Iterator<E>, Closeable{

	public static final char CSV_FIELD_DELIMITER = ',';
	public static final char CSV_EVENT_DELIMITER = '\n';
	
	public static final Factory FACTORY = new Factory();
	
	public static class Factory {
		
		private static final int DEFAULT_BUFFER_SIZE = 204800;
		
		private Factory(){}
		
		public <E> Feeder<E> createFileFeeder(Path localFile, char fieldDelimier, char eventDelimiter, Deserializer<E> deserializer, boolean memoryMap, int bufferSize) throws IOException{
			
			FileChannel channel = FileChannel.open(localFile, StandardOpenOption.READ);
			
			ByteBuffer buffer;
			if (memoryMap){
				buffer = channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size()); // ignore the buffer size as this is mapped to whatever size the file is. 
			} else{
				buffer = ByteBuffer.allocate(bufferSize);
				if (channel.read(buffer) >= bufferSize){
					throw new RuntimeException("buffer size "+bufferSize+" was not big enough to load "+localFile);
				}
				buffer.flip();
			}
			
			return new ByteBufferFeeder<E>(channel, buffer, fieldDelimier, eventDelimiter, deserializer);
		}
		
		public <E> Feeder<E> createFileFeeder(Path localFile, Deserializer<E> deserializer, boolean memoryMap) throws IOException{
			
			return createFileFeeder(localFile, CSV_FIELD_DELIMITER, CSV_EVENT_DELIMITER, deserializer, memoryMap, DEFAULT_BUFFER_SIZE);
		}
		
		public <E> Feeder<E> createFileFeeder(Path localFile, Deserializer<E> deserializer, boolean memoryMap, int bufferSize) throws IOException{
			
			return createFileFeeder(localFile, CSV_FIELD_DELIMITER, CSV_EVENT_DELIMITER, deserializer, memoryMap, bufferSize);
		}
		
		/**
		 * A byte buffer based feeder used rather than the more simple {@link java.nio.file.Files#lines(Path)} utility
		 * because this will use direct memory access avoiding the copy into the heap (and the java object creation overhead) 
		 * for the 
		 * @author haines
		 *
		 */
		private static class ByteBufferFeeder<E> implements Feeder<E> {

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
