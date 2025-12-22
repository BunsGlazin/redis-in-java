package redis;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;

public class ClientHandler implements Runnable {
	private final Socket client;
	private final ServerSocket server;
	private final Database db;
    private final RespWriter writer = new RespWriter();
    private final CommandProcessor commandProcessor = new CommandProcessor();

    public ClientHandler(Socket client, ServerSocket serverSocket, Database sharedDB) {
        this.client = client;
        this.server = serverSocket;
        this.db = sharedDB;
    }

    @Override
    public void run() {
    	try (
                BufferedReader in = new BufferedReader(new InputStreamReader(client.getInputStream()));
                BufferedWriter out = new BufferedWriter(new OutputStreamWriter(client.getOutputStream()))
            ) {
                while (true) {
                    try {
                        // Parse next RESP message
                        Value request = RespParser.readValue(in);
                        if (request == null) {
                            System.out.println("👋 Client disconnected: " + client.getInetAddress());
                            break;
                        }

                        // Ensure it's an array type (commands are always arrays)
                        if (!"array".equals(request.typ) || request.array.isEmpty()) {
                            writer.writeError(out, "invalid request");
                            out.flush();
                            continue;
                        }

                        String command = request.array.get(0).str.toUpperCase();                        
                        commandProcessor.executeCommand(command, db, writer, out, request.array);

                        out.flush();
                    }
                    catch (Exception e) {
                        System.err.println("⚠️ Internal error: " + e.getMessage());
                        writer.writeError(out, "internal server error");
                        out.flush();
                    }        
                }
            }  catch (IOException e) {
                System.err.println("⚠️ Connection error: " + e.getMessage());
            } finally {
                try {
                    client.close();
                    server.close();
                } catch (IOException e) {
                    System.err.println("⚠️ Failed to close socket: " + e.getMessage());
                }
            }
    }
}
