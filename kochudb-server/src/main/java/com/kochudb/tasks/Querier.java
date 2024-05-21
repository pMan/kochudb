package com.kochudb.tasks;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.invoke.MethodHandles;
import java.net.Socket;

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

    public Querier(Socket socket, KVStorage<ByteArray, ByteArray> storageEngine) throws IOException, ClassNotFoundException {
        this.socket = socket;
        ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
        this.req = (Request) ois.readObject();
        this.storageEngine = storageEngine;
        
        Thread.currentThread().setName("querier");
    }

    @Override
    public void run() {
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
            oos.flush();
        } catch (IOException e) {
            logger.error(e.getMessage());
            try {
                Thread.currentThread().wait(100);
                oos = new ObjectOutputStream(socket.getOutputStream());
                resp = new Response(req);
                resp.setData("Some error occurred. " + e.getMessage());
                oos.writeObject(resp);
                oos.flush();
            } catch (IOException | InterruptedException retryError) {
                retryError.printStackTrace();
            }
        }
    }

}
