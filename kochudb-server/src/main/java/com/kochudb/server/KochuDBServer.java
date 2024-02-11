package com.kochudb.server;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.invoke.MethodHandles;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.kochudb.shared.Request;
import com.kochudb.types.ByteArray;
import com.kochudb.types.KVStorage;
import com.kochudb.types.LSMTree;
import com.kochudb.util.K;

public class KochuDBServer extends Thread {
	private static final Logger logger = LogManager.getLogger(MethodHandles.lookup().lookupClass());

	private static ServerSocket serverSocket;
	
	private static boolean alive;
	
	KVStorage<ByteArray, byte[]> storageEngine;

	private static int port = 2222;
	
	public static Properties prop;
	
	public KochuDBServer(String[] args) throws IOException {
		Properties prop = new Properties();
		try {
		    prop.load(new FileInputStream("src/main/resources/application.properties"));
		} catch (IOException e) {
		    e.printStackTrace();
		}
		
		if (args.length > 1) {
			System.out.println(K.USAGE_HELP);
			logger.warn("Invalid arguments");
			System.exit(K.ERR_INVALID_CLI_ARGS);
		}
		if (args.length == 1)
			prop.setProperty("srver.port", args[0]);

		setName("socket-listener");
		
		port = Integer.parseInt(prop.getProperty("server.port"));
		
		serverSocket = new ServerSocket(port);
		storageEngine = new LSMTree(prop);
		alive = true;
	}

	public void run() {
		logger.info("Server accepting requests on :{}", port);
		while (alive) {
			try {
				Socket socket = serverSocket.accept();

				ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
				Request req = (Request) ois.readObject();

				String response = switch (req.getCommand()) {
				case "get" -> storageEngine.get(new ByteArray(req.getKey()));
				case "set" -> storageEngine.set(new ByteArray(req.getKey()), req.getValue().getBytes());
				case "del" -> storageEngine.del(new ByteArray(req.getKey()));
				default -> "Invalid Operation";
				};

				ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
				oos.writeObject(response);
			} catch (SocketException se) {
				System.out.println((alive == false) ? "Server shut down gracefully" : se.getMessage());
			} catch (Exception e) {
				logger.error(e.getMessage());
				e.printStackTrace();
				break;
			}
		}
	}

	// SIGINT+
	public void terminate() {
		alive = false;
		if (serverSocket != null) {
			try {
				logger.info("Attempting clean shut down");
				serverSocket.close();
			} catch (IOException e) {
				logger.info("Server shut down gracefully");
			}
		}
	}
}
