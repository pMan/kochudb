package com.kochudb.server;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.invoke.MethodHandles;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.kochudb.shared.Request;
import com.kochudb.shared.Response;
import com.kochudb.types.ByteArrayKey;
import com.kochudb.types.ByteArray;
import com.kochudb.types.LSMTree;

public class KochuDBServer extends Thread {

    private static final Logger logger = LogManager.getLogger(MethodHandles.lookup().lookupClass());

    private static ServerSocket serverSocket;
    
    static boolean alive;
    
    KVStorage<ByteArrayKey, ByteArray> storageEngine;

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

				try {
					byte[] response = switch (req.getCommand()) {
					case "get" -> storageEngine.get(new ByteArrayKey(req.getKey())).serialize();
					case "set" -> storageEngine.set(new ByteArrayKey(req.getKey()), new ByteArray(req.getValue()));
					case "del" -> storageEngine.del(new ByteArrayKey(req.getKey()));
					default -> "Invalid Operation".getBytes();
					};

					ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
					Response resp = new Response(req);
					resp.setData(new String(response, "utf-8"));
					oos.writeObject(resp);
				} catch (IOException e) {
					logger.error(e.getMessage());
					e.printStackTrace();

					try {
						ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
						Response resp = new Response(req);
						resp.setData("Some error occurred. " + e.getMessage());
						oos.writeObject(resp);
					} catch (IOException e1) {
						e1.printStackTrace();
					}
				}
			} catch (ClassNotFoundException | IOException e) {
				if (!alive)
					logger.info("Server shut down gracefully");
				else
					System.out.println("Server stopped accepting requests");
				break;
			}
		}
    }

    // SIGINT+
	public void terminate() throws IOException {
		alive = false;
		if (serverSocket != null) {
			logger.info("Attempting clean shut down");
			serverSocket.close();
		}
	}
}
