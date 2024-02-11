package com.kochudb.client;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ConnectException;
import java.net.Socket;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Scanner;
import java.util.Set;

import com.kochudb.shared.Request;

public class Client {
	public static boolean alive = true;
	private static final Set<String> validInput = new HashSet<>(Arrays.asList("get", "set", "del"));
	
	static Socket socket = null;
	
	public static void main(String[] args) {
		Runtime.getRuntime().addShutdownHook(new Thread() {
			public void run() {
				alive = false;
				System.out.println("\nShutting down client");
			}
		});
		
		try {
			Scanner scanner = new Scanner(System.in);
			String input = null;
			System.out.print("> ");
			while (alive && scanner.hasNextLine()) {
				//System.out.print("> ");
				input = scanner.nextLine().trim();
				
				if ("bye".equalsIgnoreCase(input) || input == null)
					break;
				
				if (input.length() < 3 || !validInput.contains(input.substring(0, 3).toLowerCase())) {
					System.out.print("> ");
					continue;
				}
				
				String[] iData = input.split(" ");
				Request req;
				switch (iData[0]) {
				case "get":
					req = new Request(iData[0], iData[1], null);
					break;
				case "set":
					req = new Request(iData[0], iData[1], iData[2]);
					break;
				case "del":
					req = new Request(iData[0], iData[1], null);
					break;
				default:
					continue;
				}
				socket = new Socket("localhost", 2222);

				ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
				oos.writeObject(req);
				
				ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
				String res = (String) ois.readObject();
				
				if (res.length() > 0)
					System.out.println(res);
				
				System.out.print("> ");
			}
			scanner.close();
			if (socket != null)
				try {
					socket.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			System.out.println("Client closed");

		} catch (ConnectException e) {
			System.out.println("Server refused connection");
		} catch (Exception e) {
			e.printStackTrace();
		}

	}
}
