package com.kochudb.server;

import static com.kochudb.k.K.DEFAULT_POOL_SIZE;
import static com.kochudb.k.K.DEFAULT_PORT;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.lang.invoke.MethodHandles;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.kochudb.shared.Request;
import com.kochudb.storage.LSMTree;
import com.kochudb.tasks.Querier;
import com.kochudb.types.ByteArray;

public class KochuDBServer extends Thread {

    private static final Logger logger = LogManager.getLogger(MethodHandles.lookup().lookupClass());

    private static ServerSocket serverSocket;
    private KVStorage<ByteArray, ByteArray> storageEngine;
    private ThreadPoolExecutor queryPool;
    private static boolean alive;

    public KochuDBServer(Properties context) throws IOException {
        setName("front-end");

        int port = Integer.parseInt(context.getProperty("server.port", DEFAULT_PORT));
        int maxParallelQueries = Integer.parseInt(context.getProperty("query.pool.size", DEFAULT_POOL_SIZE));

        serverSocket = new ServerSocket(port);
        logger.info("Server accepting connections on :{}", port);

        storageEngine = new LSMTree(context);
        queryPool = (ThreadPoolExecutor) Executors.newFixedThreadPool(maxParallelQueries);

        alive = true;
    }

    @Override
    public void run() {
        listen();
    }

    public void listen() {
        while (alive) {
            try {
                Socket socket = serverSocket.accept();
                ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
                Request req = (Request) ois.readObject();

                queryPool.submit(new Querier(socket, req, storageEngine));

            } catch (ClassNotFoundException | IOException e) {
                if (!alive)
                    logger.info("Server shut down gracefully");
                else
                    System.out.println("Server crashed");
                break;
            }
        }
        System.out.println("Bye");
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
