package com.haines.mclaren.total_transations.domain;

import java.nio.ByteBuffer;

import com.haines.mclaren.total_transations.api.Deserializer;

public class UserEventDeserializer implements Deserializer<UserEvent>{

	public static final UserEventDeserializer DESERIALIZER = new UserEventDeserializer();
	
	private UserEventDeserializer(){}
	
	@Override
	public UserEvent deserialise(ByteBuffer buffer, char fieldDelimiter, char eventDelimiter) {
		// consume from buffer until new line. We also assume there is a comer to separate the user and transaction parts
		
		String userName = readUserFromBuffer(buffer, fieldDelimiter);
		int numTransactions = readTransactionsFromBuffer(buffer, eventDelimiter);
		
		return new UserEvent.ImmutableUserEvent(userName, numTransactions);
	}
	
	private int readTransactionsFromBuffer(ByteBuffer buffer, char delimiter) {
		
		int numTransactions = 0;
		
		boolean foundDelimiter = false;
		while(!foundDelimiter && buffer.hasRemaining()){
			
			char nextChar = buffer.getChar();
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
			
			char nextChar = buffer.getChar();
			if (nextChar == delimiter){
				foundDelimiter = true; // found the delimiter, do not add it to the username
			} else{
				username.append(nextChar);
			}
		}
		
		return username.toString();
	}

}
