package com.kochudb.server;

import static com.kochudb.k.K.ERR_INVALID_CLI_ARGS;
import static com.kochudb.k.K.USAGE_HELP;
import static com.kochudb.k.K.WELCOME_BANNER;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Main class.
 */
public class Main {

    private static final Logger logger = LogManager.getLogger(MethodHandles.lookup().lookupClass());

    static Properties prop;

    public static void main(String[] args) throws IOException, FileNotFoundException {
        if (args.length > 1) {
            System.out.println(USAGE_HELP);
            logger.warn("Invalid arguments");
            System.exit(ERR_INVALID_CLI_ARGS);
        }

        prop = new Properties();

        try {
            prop.load(Main.class.getClassLoader().getResourceAsStream("application.properties"));

            // additional config file
            if (args.length == 1) {
                Properties add = new Properties();
                add.load(new FileInputStream(args[0]));
                prop.putAll(add);
            }
        } catch (IOException e) {
            //e.printStackTrace();
            throw e;
        }

        KochuDBServer kochuDBServer = new KochuDBServer(prop);

        logger.info("Register shutdown hook");

        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                try {
                	kochuDBServer.terminate();
                    kochuDBServer.join();
                } catch (InterruptedException | IOException e) {
					e.printStackTrace();
					System.out.println(e.getMessage());
                }
            }
        });

        System.out.printf((WELCOME_BANNER) + "%n", prop.getProperty("version", "0.0.0"));

        kochuDBServer.start();
    }
}
