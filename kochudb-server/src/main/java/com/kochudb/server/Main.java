package com.kochudb.server;

import java.io.IOException;
import java.lang.invoke.MethodHandles;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.kochudb.k.K;

/**
 * Main class.
 */
public class Main {

	private static final Logger logger = LogManager.getLogger(MethodHandles.lookup().lookupClass());

	public static void main(String[] args) throws NumberFormatException, IOException {

		System.out.println(K.WELCOME_BANNER);

		KochuDBServer kochuDBServer = new KochuDBServer(args);

		logger.info("Register shutdown hook");
		
		Runtime.getRuntime().addShutdownHook(new Thread() {
			public void run() {
				kochuDBServer.terminate();
				try {
					kochuDBServer.join();
				} catch (InterruptedException e) {}
			}
		});
		
		kochuDBServer.start();
	}
}
