package com.kochudb.tasks;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.invoke.MethodHandles;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.concurrent.Callable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.kochudb.server.KVStorage;
import com.kochudb.shared.DTO;
import com.kochudb.types.KochuDoc;

public class Querier implements Callable<Boolean> {

    private static final Logger logger = LogManager.getLogger(MethodHandles.lookup().lookupClass());

    private Socket socket;
    private DTO dto;
    KVStorage storageEngine;

    public Querier(Socket socket, KVStorage storageEngine) throws IOException, ClassNotFoundException {
        this.socket = socket;
        ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
        this.dto = (DTO) ois.readObject();
        this.storageEngine = storageEngine;

        Thread.currentThread().setName("querier");
    }

    @Override
    public Boolean call() {
        String command = new String(dto.command(), StandardCharsets.UTF_8);
        KochuDoc doc = switch (command) {
        case "get" -> storageEngine.get(dto.key());
        case "set" -> storageEngine.set(new KochuDoc(dto.key(), dto.value(), Instant.now().toEpochMilli()));
        case "del" -> storageEngine.del(dto.key());
        default -> new KochuDoc(null, new byte[] {}, 0L);
        };

        // add WAL here, serialize DTO
        ObjectOutputStream oos;
        DTO resp = new DTO(dto.command(), dto.key(), dto.value(), doc.getValue().bytes());

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
