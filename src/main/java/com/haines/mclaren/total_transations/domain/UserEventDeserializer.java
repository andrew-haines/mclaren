package com.haines.mclaren.total_transations.domain;

import java.nio.ByteBuffer;

import com.haines.mclaren.total_transations.api.Deserializer;
import com.haines.mclaren.total_transations.domain.UserEvent.ImmutableUserEvent;
import com.haines.mclaren.total_transations.domain.UserEvent.MutableUserEvent;

public abstract class UserEventDeserializer<T extends UserEvent> implements Deserializer<T>{

	public static final UserEventDeserializer<ImmutableUserEvent> IMMUTABLE_DESERIALIZER = new UserEventDeserializer<ImmutableUserEvent>(){

		@Override
		protected ImmutableUserEvent createUserEvent(String username, int numTransactions) {
			return new UserEvent.ImmutableUserEvent(username, numTransactions);
		}
		
	};
	public static final UserEventDeserializer<MutableUserEvent> MUTABLE_DESERIALIZER = new UserEventDeserializer<MutableUserEvent>(){

		@Override
		protected MutableUserEvent createUserEvent(String username, int numTransactions) {
			return new UserEvent.MutableUserEvent(username, numTransactions);
		}
		
	};
	
	private UserEventDeserializer(){}
	
	@Override
	public T deserialise(ByteBuffer buffer, char fieldDelimiter, char eventDelimiter) {
		// consume from buffer until new line. We also assume there is a comer to separate the user and transaction parts
		
		String userName = readUserFromBuffer(buffer, fieldDelimiter);
		int numTransactions = readTransactionsFromBuffer(buffer, eventDelimiter);
		
		return createUserEvent(userName, numTransactions);
	}
	
	protected abstract T createUserEvent(String username, int numTransactions);
	
	private int readTransactionsFromBuffer(ByteBuffer buffer, char delimiter) {
		
		int numTransactions = 0;
		
		boolean foundDelimiter = false;
		while(!foundDelimiter && buffer.hasRemaining()){
			
			char nextChar = (char)buffer.get();
			if (nextChar == delimiter){
				foundDelimiter = true; // found the delimiter, do not add it to the username
			} else if (Character.isDigit(nextChar)){ // prevents whitespace from causing issues.
				numTransactions = (numTransactions * 10) + Character.getNumericValue(nextChar);
			}
		}
		
		return numTransactions;
	}

	private String readUserFromBuffer(ByteBuffer buffer, char delimiter) {
		
		StringBuffer username = new StringBuffer();
		
		boolean foundDelimiter = false;
		while(!foundDelimiter && buffer.hasRemaining()){
			
			char nextChar = (char)buffer.get();
			if (nextChar == delimiter){
				foundDelimiter = true; // found the delimiter, do not add it to the username
			} else{
				username.append(nextChar);
			}
		}
		
		return username.toString();
	}

}
