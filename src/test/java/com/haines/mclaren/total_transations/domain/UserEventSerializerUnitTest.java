package com.haines.mclaren.total_transations.domain;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import java.nio.ByteBuffer;

import org.junit.Test;

import com.haines.mclaren.total_transations.domain.UserEvent.ImmutableUserEvent;
import com.haines.mclaren.total_transations.io.Feeder;

public class UserEventSerializerUnitTest {

	private static final String EXPECTED_SINGLE_ROW_USER_EVENT_LINE = "nc71n,2055697719\n";
	private static final String EXPECTED_MULTI_ROW_USER_EVENT_LINE = "nc71n,2055697719\n574i6,435205670\n5i1a5,1723019229\n";

	@Test
	public void givenSerializerAndSingleUserEvent_whenCallingDeserialze_thenUserEventReturned(){
		
		ByteBuffer buffer = ByteBuffer.allocate(1024);
		
		UserEventSerializer.SERIALIZER.serialise(new ImmutableUserEvent("nc71n", 2055697719), buffer, Feeder.CSV_FIELD_DELIMITER, Feeder.CSV_EVENT_DELIMITER );
		
		String result = readToString(buffer);
		
		assertThat(result, is(equalTo(EXPECTED_SINGLE_ROW_USER_EVENT_LINE)));
	}
	
	private String readToString(ByteBuffer buffer) {
		
		buffer.flip();
		
		byte[] data = new byte[buffer.remaining()];
		
		buffer.get(data);
		
		return new String(data);
	}

	@Test
	public void givenSerializerAndMultipleUserEvents_whenCallingDeserialze_thenUserEventReturned(){
		
		ByteBuffer buffer = ByteBuffer.allocate(1024);
		
		UserEventSerializer.SERIALIZER.serialise(new ImmutableUserEvent("nc71n", 2055697719), buffer, Feeder.CSV_FIELD_DELIMITER, Feeder.CSV_EVENT_DELIMITER );
		UserEventSerializer.SERIALIZER.serialise(new ImmutableUserEvent("574i6", 435205670l), buffer, Feeder.CSV_FIELD_DELIMITER, Feeder.CSV_EVENT_DELIMITER );
		UserEventSerializer.SERIALIZER.serialise(new ImmutableUserEvent("5i1a5", 1723019229l), buffer, Feeder.CSV_FIELD_DELIMITER, Feeder.CSV_EVENT_DELIMITER );
		
		String result = readToString(buffer);
		
		assertThat(result, is(equalTo(EXPECTED_MULTI_ROW_USER_EVENT_LINE)));
	}
}
