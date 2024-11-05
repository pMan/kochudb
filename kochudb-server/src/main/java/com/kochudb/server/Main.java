package com.kochudb.server;

import static com.kochudb.k.K.ERR_INVALID_CLI_ARGS;
import static com.kochudb.k.K.USAGE_HELP;
import static com.kochudb.k.K.WELCOME_BANNER;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

/**
 * Main class.
 */
public class Main {

    static Properties prop;

    public static void main(String[] args) throws IOException, FileNotFoundException {
        if (args.length > 1) {
            System.out.println(USAGE_HELP);
            System.out.println("Invalid arguments");
            System.exit(ERR_INVALID_CLI_ARGS);
        }

        prop = new Properties();

        try {
            prop.load(Main.class.getClassLoader().getResourceAsStream("application.properties"));

            // load additional config file
            if (args.length == 1) {
                Properties override = new Properties();
                override.load(new FileInputStream(args[0]));
                prop.putAll(override);
            }
        } catch (IOException e) {
            throw e;
        }

        KochuDBServer kochuDBServer = new KochuDBServer(prop);
        System.out.printf((WELCOME_BANNER) + "%n", prop.getProperty("version", "0.0.0"));

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

        kochuDBServer.start();
    }
}
