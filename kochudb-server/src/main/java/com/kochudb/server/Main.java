package com.kochudb.server;

import java.io.FileInputStream;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.kochudb.k.K;

/**
 * Main class.
 */
public class Main {

	private static final Logger logger = LogManager.getLogger(MethodHandles.lookup().lookupClass());

	static Properties prop;

	public static void main(String[] args) throws NumberFormatException, IOException {
		if (args.length > 1) {
			System.out.println(K.USAGE_HELP);
			logger.warn("Invalid arguments");
			System.exit(K.ERR_INVALID_CLI_ARGS);
		}

		prop = new Properties();
		String proFile = args.length == 1 ? args[0] : "src/main/resources/application.properties";

		System.out.println(K.WELCOME_BANNER);

		try {
			prop.load(new FileInputStream(proFile));
		} catch (IOException e) {
			e.printStackTrace();
		}

		KochuDBServer kochuDBServer = new KochuDBServer(prop);

		logger.info("Register shutdown hook");
		
		Runtime.getRuntime().addShutdownHook(new Thread() {
			public void run() {
				kochuDBServer.terminate();
				try {
					kochuDBServer.join();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		});
		
		kochuDBServer.start();
	}
}
