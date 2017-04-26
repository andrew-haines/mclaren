package com.haines.mclaren.total_transations.domain;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;

import org.junit.Test;

import com.haines.mclaren.total_transations.io.Feeder;

public class UserEventDeserializerUnitTest {
	
	private static final String SINGLE_ROW_USER_EVENT_LINE = "nc71n,2055697719";
	private static final String MULTI_ROW_USER_EVENT_LINE = "nc71n,2055697719\n574i6,435205670\n5i1a5,1723019229\n";

	@Test
	public void givenDserializerAndSingleUserEvent_whenCallingDeserialze_thenUserEventReturned(){
		UserEvent event = UserEventDeserializer.IMMUTABLE_DESERIALIZER.deserialise(getByteBuffer(SINGLE_ROW_USER_EVENT_LINE), Feeder.CSV_FIELD_DELIMITER, Feeder.CSV_EVENT_DELIMITER);
		
		assertThat(event.getUser(), is(equalTo("nc71n")));
		assertThat(event.getNumTransactions(), is(equalTo(2055697719l)));
	}
	
	@Test
	public void givenDeserializerAndMultipleUserEvents_whenCallingDeserialze_thenUserEventReturned(){
		
		ByteBuffer buffer = getByteBuffer(MULTI_ROW_USER_EVENT_LINE);
		
		UserEvent event = UserEventDeserializer.IMMUTABLE_DESERIALIZER.deserialise(buffer, Feeder.CSV_FIELD_DELIMITER, Feeder.CSV_EVENT_DELIMITER);
		
		assertThat(event.getUser(), is(equalTo("nc71n")));
		assertThat(event.getNumTransactions(), is(equalTo(2055697719l)));
		
		event = UserEventDeserializer.IMMUTABLE_DESERIALIZER.deserialise(buffer, Feeder.CSV_FIELD_DELIMITER, Feeder.CSV_EVENT_DELIMITER);
		
		assertThat(event.getUser(), is(equalTo("574i6")));
		assertThat(event.getNumTransactions(), is(equalTo(435205670l)));
		
		event = UserEventDeserializer.IMMUTABLE_DESERIALIZER.deserialise(buffer, Feeder.CSV_FIELD_DELIMITER, Feeder.CSV_EVENT_DELIMITER);
		
		assertThat(event.getUser(), is(equalTo("5i1a5")));
		assertThat(event.getNumTransactions(), is(equalTo(1723019229l)));
	}

	private ByteBuffer getByteBuffer(String events) {
		ByteBuffer buffer = ByteBuffer.allocate(1024);
		
		buffer.put(events.getBytes(Charset.forName("UTF-8")));
		
		buffer.flip();
		
		return buffer;
	}
}
