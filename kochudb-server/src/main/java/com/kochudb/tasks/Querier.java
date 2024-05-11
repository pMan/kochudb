package com.kochudb.tasks;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.lang.invoke.MethodHandles;
import java.net.Socket;
import java.util.concurrent.locks.LockSupport;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.kochudb.server.KVStorage;
import com.kochudb.shared.Request;
import com.kochudb.shared.Response;
import com.kochudb.types.ByteArray;

public class Querier implements Runnable {

    private static final Logger logger = LogManager.getLogger(MethodHandles.lookup().lookupClass());

    private Socket socket;
    private Request req;
    KVStorage<ByteArray, ByteArray> storageEngine;

    public Querier(Socket socket, Request request, KVStorage<ByteArray, ByteArray> storageEngine) {
        this.socket = socket;
        this.req = request;
        this.storageEngine = storageEngine;
    }

    @Override
    public void run() {
        Thread.currentThread().setName("querier");
        byte[] response = switch (req.getCommand()) {
        case "get" -> storageEngine.get(new ByteArray(req.getKey())).serialize();
        case "set" -> storageEngine.set(new ByteArray(req.getKey()), new ByteArray(req.getValue()));
        case "del" -> storageEngine.del(new ByteArray(req.getKey()));
        default -> "Invalid Operation".getBytes();
        };

        ObjectOutputStream oos;
        Response resp = new Response(req);

        try {
            oos = new ObjectOutputStream(socket.getOutputStream());
            resp.setData(new String(response, "utf-8"));
            oos.writeObject(resp);

        } catch (IOException e) {
            logger.error(e.getMessage());
            logger.info("Retrying in 500 ns");

            LockSupport.parkNanos(500);

            try {
                oos = new ObjectOutputStream(socket.getOutputStream());
                resp = new Response(req);
                resp.setData("Some error occurred. " + e.getMessage());
                oos.writeObject(resp);
            } catch (IOException retryError) {
                retryError.printStackTrace();
            }
        }
    }

}
