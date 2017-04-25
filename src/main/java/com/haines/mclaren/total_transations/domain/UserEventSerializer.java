package com.haines.mclaren.total_transations.domain;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;

import com.haines.mclaren.total_transations.api.Serializer;

public class UserEventSerializer implements Serializer<UserEvent>{

	public static final UserEventSerializer SERIALIZER = new UserEventSerializer();
	
	private UserEventSerializer(){}
	
	@Override
	public void serialise(UserEvent event, ByteBuffer buffer, char fieldDelimiter, char eventDelimiter) {
		
		buffer.put(event.getUser().getBytes(Charset.forName("UTF-8")));
		buffer.putChar(fieldDelimiter);
		
		/* 
		 * write out long as these aggregations could 2B (32bit) transactions may not be enough. Might seem excessive but
		 * if these transactions represent web engagement activity (scroll events etc) then this can get to this sort of
		 * level if the period of aggregation is large enough.
		 */ 
		buffer.putLong(event.getNumTransactions()); 
	}
}
