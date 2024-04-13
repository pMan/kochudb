package com.kochudb.server;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.lang.invoke.MethodHandles;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.kochudb.shared.Request;
import com.kochudb.storage.LSMTree;
import com.kochudb.tasks.Querier;
import com.kochudb.types.ByteArray;

public class KochuDBServer extends Thread {

    private static final Logger logger = LogManager.getLogger(MethodHandles.lookup().lookupClass());

    private static ServerSocket serverSocket;
    
    static boolean alive;
    
    KVStorage<ByteArray, ByteArray> storageEngine;

    private static int port = 2222;
    
    ExecutorService executor;
    
    public KochuDBServer(Properties context) throws IOException {
        setName("front-end");
        
        port = Integer.parseInt(context.getProperty("server.port"));
        
        serverSocket = new ServerSocket(port);
        storageEngine = new LSMTree(context);
        executor = Executors.newFixedThreadPool(25);
        
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

				executor.submit(new Querier(socket, req, storageEngine));
				
			} catch (ClassNotFoundException | IOException e) {
				if (!alive)
					logger.info("Server shut down gracefully");
				else
					System.out.println("Server crashed");
				break;
			}
		}
		System.out.println("Server shut down");
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
