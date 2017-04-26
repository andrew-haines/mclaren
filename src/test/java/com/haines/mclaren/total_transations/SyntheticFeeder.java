package com.haines.mclaren.total_transations;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.logging.Logger;

import com.haines.mclaren.total_transations.domain.UserEvent;
import com.haines.mclaren.total_transations.domain.UserEvent.ImmutableUserEvent;
import com.haines.mclaren.total_transations.io.Feeder;

public class SyntheticFeeder implements Feeder<UserEvent>{

	public static final List<String> DEFAULT_LONG_TAIL = Arrays.asList("HAINES", "MCLAREN", "1234aBc", "AHaines", "Andy", "abC1234", "4321abc");

	
	/*
	 * with 5 max upper case digits it means a 1 / n!/(n-k)! = 24! / (24 - 5)! = 1/ 5100480 chance of a collision. 
	 * As we only create a million events there is an unlikely chance that this will generate a user to aggregate.
	 * Consequently i restrict my alphabet to the following 8 character alphabet (and hence a chance of getting a duplicate)
	 * user event being 1 / 6720
	 */
	public static final int TEST_MAX_USER_NAME_LENGTH = 5;
	
	public static final char[] SMALL_ALPHABET = new char[]{'a', 'h', 'i', 'n', 'e', 's', 'm', 'c'};
	
	/*
	 * The following uses an alphabet of 16 characters which makes the event of a collision much more unlikely. Excluding the
	 * long tail list, the chance is 1 / 524160 when choosing 5 character length user names
	 */
	
	public static final char[] LARGE_ALPHABET = new char[]{'a', 'h', 'i', 'n', 'e', 's', 'm', 'c', '1', '2', '3', '4', '5', '6', '7', '8'};

	
	private static final Logger LOG = Logger.getLogger(SyntheticFeeder.class.getName());
	
	private final int numEvents;
	private int currentEventNumber;
	private final Random rnd;
	private final double chanceOfPickingLongTail;
	private final List<String> longTailUserList;
	private final int maxUserNameLength;
	private final char[] alphabet;
	
	public SyntheticFeeder(int numEvents, long startSeed, List<String> longTailUserList, double chanceOfPickingLongTail, int maxUserNameLength, char[] alphabet) {
		this.numEvents = numEvents;
		this.rnd = new Random(startSeed);
		this.chanceOfPickingLongTail = chanceOfPickingLongTail;
		this.longTailUserList = longTailUserList;
		this.maxUserNameLength = maxUserNameLength;
		this.alphabet = alphabet;
	}

	@Override
	public boolean hasNext() {
		return currentEventNumber < numEvents;
	}

	@Override
	public UserEvent next() {
		try{
			UserEvent event = generateNextEvent();
			//LOG.log(Level.INFO, "generated new event: "+event.toString());
			
			return event;
			
		}finally{
			currentEventNumber++;
		}
	}

	private UserEvent generateNextEvent() {
		return new ImmutableUserEvent(generateRandomUser(), generateRandomNumTransactions());
	}

	private String generateRandomUser() {
		
		// first see if we are using a user from the long tail
		
		double longTail = rnd.nextDouble();
		
		if (longTail < chanceOfPickingLongTail){
			return pickFromLongTail();
		} else{
			return generateRandomString();
		}
	}

	private String generateRandomString() {
		StringBuilder builder = new StringBuilder();
		
		for (int i = 0; i < maxUserNameLength; i++){
			builder.append(alphabet[Math.abs(rnd.nextInt() % alphabet.length)]);
		}
		
		return builder.toString();
	}

	private String pickFromLongTail() {
		return longTailUserList.get(Math.abs(rnd.nextInt() % longTailUserList.size()));
	}

	private long generateRandomNumTransactions() {
		return Math.abs(rnd.nextInt());
	}

	@Override
	public void close() throws IOException {
		// no op
	}
}