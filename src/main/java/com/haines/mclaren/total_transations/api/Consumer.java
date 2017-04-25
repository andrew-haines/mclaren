package com.haines.mclaren.total_transations.api;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;

public interface Consumer<E> extends Closeable {
	
	boolean consume(E event);
	
	public static class SeperateThreadConsumer<E> implements Consumer<E>, Runnable{
		
		private static final Logger LOG = Logger.getLogger(SeperateThreadConsumer.class.getName());
		
		private final Consumer<E> actualConsumer;
		private final BlockingQueue<E> eventsQueue;
		private final AtomicBoolean isRunning;
		private final AtomicReference<String> runningThreadName;
		
		public SeperateThreadConsumer(Consumer<E> actualConsumer){
			this.eventsQueue = new LinkedBlockingQueue<E>();
			this.actualConsumer = actualConsumer;
			this.isRunning = new AtomicBoolean(false);
			this.runningThreadName = new AtomicReference<String>("Not set yet");
		}

		public void run() {
			
			Thread currentThread = Thread.currentThread(); // the consumer thread.
			runningThreadName.set(currentThread.getName());
			
			LOG.log(Level.INFO, "Starting new consumer thread on "+runningThreadName.get());
			
			isRunning.set(true);
			
			while((isRunning.get() && !Thread.interrupted()) || !eventsQueue.isEmpty()){ // always purge any existing queue entries even when closed
				
				processQueue();
			}
			
			LOG.log(Level.INFO, "Stopping consumer thread on " +runningThreadName.get());
			
			try {
				actualConsumer.close();
			} catch (IOException e) {
				throw new RuntimeException("unable to close on consumer thread: "+runningThreadName.get(), e);
			}
		}

		private void processQueue(){
			try{
				E event = eventsQueue.take();
				actualConsumer.consume(event);
			} catch (InterruptedException e){
				Thread.currentThread().interrupt();
				LOG.log(Level.INFO, "Thread {} interrupted", runningThreadName.get());
			} catch (RuntimeException e){
				LOG.log(Level.SEVERE, "Thread "+runningThreadName.get()+" encountered an uncaught exception", e);
			}
		}

		public boolean consume(E e) {
			if (isRunning.get()){
				return eventsQueue.offer(e); // is on the producer thread.
			} else{
				LOG.log(Level.WARNING, "Recieving events still even though consumer on thread: "+runningThreadName.get()+" has been closed");
				return false;
			}
		}

		@Override
		public void close() throws IOException {
			isRunning.set(true);
			
			// dont close the actual consumer here, do so on the running thread as this has ownership
		}
	}
	
	public static class ChainedConsumer<E extends Event<E>> implements Consumer<E>{

		private final Collection<Consumer<E>> consumers;
		
		public ChainedConsumer(Collection<Consumer<E>> consumers){
			this.consumers = consumers;
		}
		
		@Override
		public void close() throws IOException {
			for (Consumer<E> consumer: consumers){
				consumer.close();
			}
		}

		@Override
		public boolean consume(E event) {
			boolean result = true;
			
			for (Consumer<E> consumer: consumers){
				result = result & consumer.consume(event);
			}
			
			return result;
		}
		
		@SafeVarargs
		public static <E extends Event<E>> ChainedConsumer<E> chain(Consumer<E>... consumers){
			return new ChainedConsumer<>(Arrays.asList(consumers));
		}
	}
}
