package com.kochudb.server;

import static com.kochudb.k.K.DEFAULT_POOL_SIZE;
import static com.kochudb.k.K.DEFAULT_PORT;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.SocketException;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

import com.kochudb.storage.LSMTree;
import com.kochudb.tasks.Querier;
import com.kochudb.types.ByteArray;

public class KochuDBServer extends Thread {

    private static ServerSocket serverSocket;
    private KVStorage<ByteArray, ByteArray> storageEngine;
    private ThreadPoolExecutor queryPool;
    private static boolean alive;

    public KochuDBServer(Properties context) throws IOException {
        setName("front-end");

        int port = Integer.parseInt(context.getProperty("server.port", DEFAULT_PORT));
        int maxParallelQueries = Integer.parseInt(context.getProperty("query.pool.size", DEFAULT_POOL_SIZE));

        serverSocket = new ServerSocket(port);
        System.out.println("Server accepting connections on " + port);

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
                queryPool.submit(new Querier(serverSocket.accept(), storageEngine));
            } catch (ClassNotFoundException | IOException e) {
                if (e instanceof SocketException)
                    System.out.println(e.getMessage());
                else
                    System.out.println("Server encountered an error.");
            }
        }
        if (!alive) {
            System.out.println("Server shut down gracefully");
        }
    }

    // SIGINT+
    public void terminate() throws IOException {
        alive = false;
        if (serverSocket != null) {
            System.out.println("Attempting clean shut down");
            serverSocket.close();
        }
    }
}
