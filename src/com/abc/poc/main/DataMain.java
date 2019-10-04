package com.abc.poc.main;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is the main class to start the application.
 * @author Manoj Bansal
 *
 */
public class DataMain {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(DataMain.class);

	public static void main(String[] args) throws Exception {
		LOGGER.info("Application starting.");
		String[] producersId = new String[]{"A","B","C","D"};

		int bufferSize = 1024;
		
		int totalEvents = 50;
		
		MessagingEngine engine = new MessagingEngine();
		LOGGER.info("Start messaging engine...");
		engine.start(producersId, bufferSize, totalEvents);
	}
}

