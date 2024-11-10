package com.kochudb.tasks;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.invoke.MethodHandles;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Callable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.kochudb.server.KVStorage;
import com.kochudb.shared.DTO;
import com.kochudb.types.ByteArray;

public class Querier implements Callable<Boolean> {

    private static final Logger logger = LogManager.getLogger(MethodHandles.lookup().lookupClass());

    private Socket socket;
    private DTO dto;
    KVStorage<ByteArray, ByteArray> storageEngine;

    public Querier(Socket socket, KVStorage<ByteArray, ByteArray> storageEngine)
            throws IOException, ClassNotFoundException {
        this.socket = socket;
        ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
        this.dto = (DTO) ois.readObject();
        this.storageEngine = storageEngine;

        Thread.currentThread().setName("querier");
    }

    @Override
    public Boolean call() {
        String command = new String(dto.command(), StandardCharsets.UTF_8);
        byte[] response = switch (command) {
        case "get" -> storageEngine.get(new ByteArray(dto.key())).bytes();
        case "set" -> storageEngine.set(new ByteArray(dto.key()), new ByteArray(dto.value()));
        case "del" -> storageEngine.del(new ByteArray(dto.key()));
        default -> "Invalid Operation".getBytes();
        };

        // add WAL here, serialize DTO
        ObjectOutputStream oos;
        DTO resp = new DTO(dto.command(), dto.key(), dto.value(), response);

        try {
            oos = new ObjectOutputStream(socket.getOutputStream());
            oos.writeObject(resp);
            oos.flush();
        } catch (IOException e) {
            logger.error(e.getMessage());
            return false;
        }
        return true;
    }

}
