package com.haines.mclaren.total_transations.domain;

import java.io.Serializable;
import java.util.Comparator;

import com.haines.mclaren.total_transations.api.Event;

public abstract class UserEvent implements Event<UserEvent> {

	public static final Comparator<UserEvent> RANKED_BY_TRANSACTIONS = new Comparator<UserEvent>(){

		@Override
		public int compare(UserEvent o1, UserEvent o2) {
			return Long.compare(o1.getNumTransactions(),  o2.getNumTransactions());
		}
	};
	
	private final String user;
	
	public UserEvent(String user){
		this.user = user;
	}

	public String getUser() {
		return user;
	}
	

	@Override
	public Serializable getAggregationValue() {
		return user;
	}
	
	@Override
	public String toString(){
		return "UserEvent{user: "+user+",numTranactions: "+getNumTransactions()+"}";
	}
	
	@Override
	public int hashCode(){
		return user.hashCode(); // just hash on user only
	}
	
	public boolean equals(Object o){
		if (o instanceof UserEvent){
			UserEvent other = (UserEvent)o;
			
			if (this.user.equals(other.user) && this.getNumTransactions() == other.getNumTransactions()){
				return true;
			}
		}
		return false;
	}

	public abstract long getNumTransactions();
	
	public static class MutableUserEvent extends UserEvent implements MutableEvent<UserEvent>{
		
		// this should only be updated by the same thread that constructed it. This follows the single writer paradigm.
		// precision is long as it could be many multiple events per user of large int numbers.
		private long numTransactions;

		public MutableUserEvent(String user, long numTransactions){
			super(user);
			
			this.numTransactions = numTransactions;
		}
		
		@Override
		public void aggregate(UserEvent event){
			assert this.getUser().equals(event.getUser()) : "User's dont match. u1: "+this.getUser()+", u2: "+event.getUser();
			
			this.numTransactions =+ event.getNumTransactions();
		}

		@Override
		public long getNumTransactions() {
			return numTransactions;
		}

		@Override
		public UserEvent toImmutableEvent() {
			return new ImmutableUserEvent(this);
		}

		@Override
		public MutableEvent<UserEvent> toMutableEvent() {
			return this;
		}
	}
	
	public static class ImmutableUserEvent extends UserEvent {

		private final long numTransactions;
		
		private ImmutableUserEvent(MutableUserEvent mutableEvent){
			this(mutableEvent.getUser(), mutableEvent.numTransactions);
		}
		
		public ImmutableUserEvent(String user, long numTransactions){
			super(user);
			
			this.numTransactions = numTransactions;
		}
		
		@Override
		public long getNumTransactions() {
			return numTransactions;
		}

		@Override
		public UserEvent toImmutableEvent() {
			return this;
		}

		@Override
		public MutableEvent<UserEvent> toMutableEvent() {
			return new MutableUserEvent(this.getUser(), numTransactions);
		}
	}
}
