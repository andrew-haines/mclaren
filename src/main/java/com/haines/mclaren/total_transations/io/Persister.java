package com.haines.mclaren.total_transations.io;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import com.haines.mclaren.total_transations.api.Consumer;
import com.haines.mclaren.total_transations.api.Event;
import com.haines.mclaren.total_transations.api.Serializer;

public interface Persister<E extends Event<E>> extends Consumer<E>{

	public static final Factory FACTORY = new Factory();
	
	public static class Factory {
		
		private Factory(){}
		
		public <E extends Event<E>> Persister<E> createCSVPersister(Path outputFile, Serializer<E> serializer) throws IOException{
			return createDelimitedPersister(outputFile, serializer, Feeder.CSV_FIELD_DELIMITER, Feeder.CSV_EVENT_DELIMITER);
		}

		private <E extends Event<E>> Persister<E> createDelimitedPersister(Path outputFile, Serializer<E> serializer, char csvFieldDelimiter, char csvEventDelimiter) throws IOException {
			
			FileChannel channel = FileChannel.open(outputFile, StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW);
			
			ByteBuffer buffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, channel.size());
			
			return new DelimitedByteBufferPersister<E>(channel, buffer, csvFieldDelimiter, csvEventDelimiter, serializer);
		}
		
		private static class DelimitedByteBufferPersister<E extends Event<E>> implements Persister<E>{

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
