package com.kochudb.client;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Scanner;
import java.util.Set;

import com.kochudb.shared.Request;
import com.kochudb.shared.Response;

public class Client {
    public static boolean alive = true;

    private static final Set<String> validInput = new HashSet<>(Arrays.asList("get", "set", "del"));

    private static final String usage = "|| KochuDB client ||\nUsage help:\n\tset <key> <val>\n\tget <key>\n\tdel <key>\n\n";

    static Socket socket = null;

    public static void main(String[] args) {
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

                String[] iData = input.split(" ");
                Request req = createReq(input);

                socket = new Socket("localhost", 2222);

                ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
                oos.writeObject(req);

                ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
                Response res = (Response) ois.readObject();

                if (!res.isEmpty())
                    System.out.println(res.getData());

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

    static Request createReq(String input) {
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

        Request req = switch (op) {
        case "get", "del" -> new Request(op, key, null);
        case "set" -> new Request(op, key, val);
        default -> throw new IllegalArgumentException("Unexpected operation: " + op);
        };

        return req;
    }

}
