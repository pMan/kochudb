package com.kochudb.tasks;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.invoke.MethodHandles;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.kochudb.server.KVStorage;
import com.kochudb.shared.DTO;
import com.kochudb.types.ByteArray;

public class Querier implements Runnable {

    private static final Logger logger = LogManager.getLogger(MethodHandles.lookup().lookupClass());

    private Socket socket;
    private DTO dTO;
    KVStorage<ByteArray, ByteArray> storageEngine;

    public Querier(Socket socket, KVStorage<ByteArray, ByteArray> storageEngine)
            throws IOException, ClassNotFoundException {
        this.socket = socket;
        ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
        this.dTO = (DTO) ois.readObject();
        this.storageEngine = storageEngine;

        Thread.currentThread().setName("querier");
    }

    @Override
    public void run() {
        String command = new String(dTO.command(), StandardCharsets.UTF_8);
        byte[] response = switch (command) {
        case "get" -> storageEngine.get(new ByteArray(dTO.key())).serialize();
        case "set" -> storageEngine.set(new ByteArray(dTO.key()), new ByteArray(dTO.value()));
        case "del" -> storageEngine.del(new ByteArray(dTO.key()));
        default -> "Invalid Operation".getBytes();
        };

        ObjectOutputStream oos;
        DTO resp = new DTO(dTO.command(), dTO.key(), dTO.value(), response);

        try {
            oos = new ObjectOutputStream(socket.getOutputStream());
            // resp.setData(new String(response, "utf-8"));
            oos.writeObject(resp);
            oos.flush();
        } catch (IOException e) {
            logger.error(e.getMessage());
        }
    }

}
