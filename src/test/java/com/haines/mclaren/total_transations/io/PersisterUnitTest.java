package com.haines.mclaren.total_transations.io;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import com.haines.mclaren.total_transations.domain.UserEvent;
import com.haines.mclaren.total_transations.domain.UserEvent.MutableUserEvent;
import com.haines.mclaren.total_transations.domain.UserEventSerializer;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class PersisterUnitTest {
	
	private static final String TEST_OUTPUT = "persistantUnitTest.dat";
	
	private static final List<UserEvent> TEST_EVENTS = Arrays.asList(new MutableUserEvent("1234", 565333l),
																	new MutableUserEvent("4321", 7756443l),
																	new MutableUserEvent("957ddd", 56723l),
																	new MutableUserEvent("bjfweuho", 885532l),
																	new MutableUserEvent("oopshw", 3341l),
																	new MutableUserEvent("ood742", 42l));

	private static final String EXPECTED_OUTPUT = "1234,565333\n4321,7756443\n957ddd,56723\nbjfweuho,885532\noopshw,3341\nood742,42\n";
	
	private Persister<UserEvent> candidate;
	
	@Before
	public void before() throws IOException, URISyntaxException{
		
		Path testPath = getTestPath(TEST_OUTPUT);
		
		Files.deleteIfExists(testPath);
		
		candidate = Persister.FACTORY.createCSVPersister(testPath, UserEventSerializer.SERIALIZER, false, 1024);
	}
	
	@Test
	public void givenMultipleEvents_whenCandidateConsumesThemAll_thenFileCreatedAsExpected() throws IOException, URISyntaxException{
		TEST_EVENTS.stream().forEach(e -> candidate.consume(e));
		
		candidate.close();
		
		String contents = new String(Files.readAllBytes(getTestPath(TEST_OUTPUT)), Charset.forName("UTF-8"));
		
		assertThat(contents, is(equalTo(EXPECTED_OUTPUT)));
	}
	
	@Test
	public void givenMultipleEvents_whenMemoryMappedCandidateConsumesThemAll_thenFileCreatedAsExpected() throws IOException, URISyntaxException{
		Path testPath = getTestPath(TEST_OUTPUT);
		Files.deleteIfExists(testPath);
		Persister<UserEvent> candidate = Persister.FACTORY.createCSVPersister(testPath, UserEventSerializer.SERIALIZER, true, 64);
		
		TEST_EVENTS.stream().forEach(e -> candidate.consume(e));
		
		candidate.close();
		
		String contents = new String(Files.readAllBytes(testPath), Charset.forName("UTF-8"));
		
		assertThat(contents, is(equalTo(EXPECTED_OUTPUT)));
	}

	private static Path getTestPath(String pathName) throws URISyntaxException {
		URI rootDir = PersisterUnitTest.class.getResource("/").toURI();
		
		
		return Paths.get(rootDir.getPath(), pathName);
	}
}
