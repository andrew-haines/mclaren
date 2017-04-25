package com.haines.mclaren.total_transations.api;

import java.io.IOException;

import com.haines.mclaren.total_transations.domain.UserEvent;

public interface DomainFactory {

	public Consumer<UserEvent> createInitalChainConsumer() throws IOException, ClassNotFoundException;
}
