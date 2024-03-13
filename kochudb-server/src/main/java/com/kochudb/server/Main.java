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

        try {
            prop.load(Main.class.getClassLoader().getResourceAsStream("application.properties"));

            // additional config file
            if (args.length == 1) {
                Properties add = new Properties();
                add.load(new FileInputStream(args[0]));
                prop.putAll(add);
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(6);
        }

        KochuDBServer kochuDBServer = new KochuDBServer(prop);

        logger.info("Register shutdown hook");

        System.exit(9);
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

        System.out.printf((K.WELCOME_BANNER) + "%n", prop.getProperty("version", "0.0.0"));

        kochuDBServer.start();
    }
}
