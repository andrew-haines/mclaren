package com.haines.mclaren.total_transations.domain;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;

import com.haines.mclaren.total_transations.api.Serializer;
import com.haines.mclaren.total_transations.domain.UserEvent.MutableUserEvent;

public abstract class UserEventSerializer<E extends UserEvent> implements Serializer<E>{

	private static final Charset UTF8 = Charset.forName("UTF-8");
	
	public static final UserEventSerializer<UserEvent> SERIALIZER = new UserEventSerializer<UserEvent>(){};
	
	public static final UserEventSerializer<MutableUserEvent> MUTABLE_SERIALIZER = new UserEventSerializer<MutableUserEvent>(){};
	
	private UserEventSerializer(){}
	
	@Override
	public void serialise(E event, ByteBuffer buffer, char fieldDelimiter, char eventDelimiter) {
		
		buffer.put(event.getUser().getBytes(UTF8));
		buffer.put((fieldDelimiter+"").getBytes(UTF8)); //string pool will optimize here
		
		/* 
		 * write out long as these aggregations could 2B (32bit) transactions may not be enough. Might seem excessive but
		 * if these transactions represent web engagement activity (scroll events etc) then this can get to this sort of
		 * level if the period of aggregation is large enough.
		 */ 
		buffer.put((event.getNumTransactions()+"").getBytes(UTF8));
		buffer.put((eventDelimiter+"").getBytes(UTF8)); //string pool will optimize here
	}
}
