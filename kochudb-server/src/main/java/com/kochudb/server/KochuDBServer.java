package com.kochudb.server;

import static com.kochudb.k.K.DEFAULT_POOL_SIZE;
import static com.kochudb.k.K.DEFAULT_PORT;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.LinkedList;
import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;

import com.kochudb.storage.LSMTree;
import com.kochudb.tasks.Querier;
import com.kochudb.types.ByteArray;

public class KochuDBServer extends Thread {

    private static ServerSocket serverSocket;
    private KVStorage<ByteArray, ByteArray> storageEngine;
    private ThreadPoolExecutor queryPool;
    private static boolean alive;
    private Queue<Future<Boolean>> queryResults;

    public KochuDBServer(Properties context) throws IOException {
        setName("front-end");

        int port = Integer.parseInt(context.getProperty("server.port", DEFAULT_PORT));
        int maxParallelQueries = Integer.parseInt(context.getProperty("query.pool.size", DEFAULT_POOL_SIZE));

        serverSocket = new ServerSocket(port);
        System.out.println("Server accepting connections on " + port);

        storageEngine = new LSMTree(context);
        queryPool = (ThreadPoolExecutor) Executors.newFixedThreadPool(maxParallelQueries);
        queryResults = new LinkedList<Future<Boolean>>();

        alive = true;
    }

    @Override
    public void run() {
        listen();
    }

    public void listen() {
        while (alive) {
            try {
                Future<Boolean> future = queryPool.submit(new Querier(serverSocket.accept(), storageEngine));
                queryResults.offer(future);
            } catch (ClassNotFoundException e) {
                System.out.println("Server encountered ClassNotFound exception.");
                e.printStackTrace();
            } catch (IOException e) {
                System.out.println(e.getMessage());
            } finally {
                alive = false;
            }

            // remove all completed futures
            while (!queryResults.isEmpty() && queryResults.peek().isDone())
                queryResults.poll();
        }

        if (!alive) {
            // wait for all running queries to complete to ensure graceful shutdown
            while (!queryResults.isEmpty()) {
                try {
                    queryResults.poll().get();
                } catch (InterruptedException | ExecutionException e) {
                    e.printStackTrace();

                    if (e instanceof InterruptedException)
                        Thread.currentThread().interrupt();
                }
            }
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
