package com.haines.mclaren.total_transations.io;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Util {

	private static final Logger LOG = Logger.getLogger(Util.class.getName());
	
	public static void recursiveDelete(Path localFile) throws IOException {
		
		if (Files.isDirectory(localFile)){
			DirectoryStream<Path> contents = Files.newDirectoryStream(localFile);
			
			for (Path content: contents){
				recursiveDelete(content); // dfs
			}
		}
		LOG.log(Level.INFO, "Deleting directory: " + localFile);
		
		Files.deleteIfExists(localFile);
	}
}
