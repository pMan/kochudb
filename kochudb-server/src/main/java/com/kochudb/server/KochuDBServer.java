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

import com.kochudb.k.K;
import com.kochudb.shared.Request;
import com.kochudb.shared.Response;
import com.kochudb.types.ByteArray;
import com.kochudb.types.LSMTree;

public class KochuDBServer extends Thread {

	private static final Logger logger = LogManager.getLogger(MethodHandles.lookup().lookupClass());

	private static ServerSocket serverSocket;
	
	private static boolean alive;
	
	KVStorage<ByteArray, byte[]> storageEngine;

	private static int port = 2222;
	
	//public static Properties prop;
	
	public KochuDBServer(Properties context) throws IOException {
		setName("front-end");
		
		port = Integer.parseInt(context.getProperty("server.port"));
		
		serverSocket = new ServerSocket(port);
		storageEngine = new LSMTree(context);
		alive = true;
	}

	@Override
	public void run() {
		logger.info("Server accepting requests on :{}", port);
		listen();
	}

	public void listen() {
		while (alive) {
			try {
				Socket socket = serverSocket.accept();

				ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
				Request req = (Request) ois.readObject();

				byte[] response = switch (req.getCommand()) {
				case "get" -> storageEngine.get(new ByteArray(req.getKey()));
				case "set" -> storageEngine.set(new ByteArray(req.getKey()), req.getValue().getBytes());
				case "del" -> storageEngine.del(new ByteArray(req.getKey()));
				default -> "Invalid Operation".getBytes();
				};

				ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
				Response resp = new Response(req);
				resp.setData(new String(response, "utf-8"));
				oos.writeObject(resp);
			} catch (SocketException se) {
				System.out.println((!alive) ? "Server shut down gracefully" : se.getMessage());
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
