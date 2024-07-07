package com.kochudb.client;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Scanner;
import java.util.Set;

import com.kochudb.shared.DTO;

public class Client {
    public static boolean alive = true;

    private static final Set<String> validInput = new HashSet<>(Arrays.asList("get", "set", "del"));

    private static final String usage = "<< KochuDB CLI client >>\nUsage help:\n\tset <key> <val>\n\tget <key>\n\tdel <key>\n\n";

    static Socket socket = null;

    public static void main(String[] args) throws UnknownHostException, ClassNotFoundException, IOException {
        // load test data
        if (args.length == 1 && "load".equals(args[0])) {
            WriteThroughputTest.main(args);
            return;
        }
        
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                alive = false;
                System.out.println("\nShutting down client");
            }
        });

        System.out.print(usage);
        System.out.print("> Type \"bye\" to exit\n");

        Scanner scanner = new Scanner(System.in);
        String input = null;

        System.out.print("> ");

        while (alive && scanner.hasNextLine()) {
            try {
                input = scanner.nextLine().trim();

                if ("bye".equalsIgnoreCase(input) || input == null)
                    break;

                if (input.length() < 3 || !validInput.contains(input.substring(0, 3).toLowerCase())) {
                    System.out.print("> ");
                    continue;
                }

                DTO dTO = createReq(input);
                socket = new Socket("localhost", 2222);

                ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
                oos.writeObject(dTO);

                ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
                DTO res = (DTO) ois.readObject();

                System.out.println(new String(res.response(), StandardCharsets.UTF_8));

                System.out.print("> ");
            } catch (Exception e) {
                System.out.println("Invalid input\n> ");
            }
        }
        scanner.close();
        if (socket != null)
            try {
                socket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        System.out.println("Client closed");

    }

    static DTO createReq(String input) {
        if (input.length() < 3 || input.charAt(3) != ' ')
            throw new IllegalArgumentException("Invalid input");

        String op = input.substring(0, 3), key = null, val = null;

        int i;
        for (i = 4; i < input.length(); i++) {
            char c = input.charAt(i);
            if (c == ' ' && key == null) {
                key = input.substring(4, i);
                val = input.substring(i).trim();
                break;
            }
        }

        if (key == null)
            key = input.substring(3).trim();

        DTO dto = switch (op) {
        case "get", "del" -> new DTO(op, key, null);
        case "set" -> new DTO(op, key, val.getBytes());
        default -> throw new IllegalArgumentException("Unexpected operation: " + op);
        };

        return dto;
    }

}
