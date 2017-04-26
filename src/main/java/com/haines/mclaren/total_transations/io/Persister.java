package com.haines.mclaren.total_transations.io;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import com.haines.mclaren.total_transations.api.Consumer;
import com.haines.mclaren.total_transations.api.Serializer;

public interface Persister<E> extends Consumer<E>{
	
	public static final Factory FACTORY = new Factory();
	
	public static class Factory {
		
		private static final int DEFAULT_BUFFER_SIZE = 204800;

		private Factory(){}
		
		public <E> Persister<E> createCSVPersister(Path outputFile, Serializer<E> serializer, boolean memoryMap) throws IOException{
			return createDelimitedPersister(outputFile, serializer, Feeder.CSV_FIELD_DELIMITER, Feeder.CSV_EVENT_DELIMITER, memoryMap, DEFAULT_BUFFER_SIZE);
		}
		
		public <E> Persister<E> createCSVPersister(Path outputFile, Serializer<E> serializer, boolean memoryMap, int bufferSize) throws IOException{
			return createDelimitedPersister(outputFile, serializer, Feeder.CSV_FIELD_DELIMITER, Feeder.CSV_EVENT_DELIMITER, memoryMap, bufferSize);
		}

		private <E> Persister<E> createDelimitedPersister(Path outputFile, Serializer<E> serializer, char csvFieldDelimiter, char csvEventDelimiter, boolean memoryMap, int bufferSize) throws IOException {
			
			FileChannel channel = FileChannel.open(outputFile, StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
			
			ByteBuffer buffer;
			if (memoryMap){
				buffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, bufferSize);
			} else{
				buffer = ByteBuffer.allocate(bufferSize);
			}
			
			return new DelimitedByteBufferPersister<E>(channel, buffer, csvFieldDelimiter, csvEventDelimiter, serializer){
				
				@Override
				public void close() throws IOException {
					if (!memoryMap){ // if we are not using memory mapped buffers then write out to channel before closing
						buffer.flip();
						channel.write(buffer);
					}
					
					super.close();
				}
			};
		}
		
		private static class DelimitedByteBufferPersister<E> implements Persister<E>{

			private final Closeable channel;
			private final ByteBuffer buffer;
			private final char fieldDelimiter;
			private final char eventDelimiter;
			private final Serializer<E> serializer;
			
			public DelimitedByteBufferPersister(Closeable channel, ByteBuffer buffer, char fieldDelimiter, char eventDelimiter, Serializer<E> serializer) {
				this.channel = channel;
				this.buffer = buffer;
				this.fieldDelimiter = fieldDelimiter;
				this.eventDelimiter = eventDelimiter;
				this.serializer = serializer;
			}

			@Override
			public boolean consume(E event) {
				
				serializer.serialise(event, buffer, fieldDelimiter, eventDelimiter);
				
				return true;
			}

			@Override
			public void close() throws IOException {
				channel.close();
			}
			
		}
	}
}
