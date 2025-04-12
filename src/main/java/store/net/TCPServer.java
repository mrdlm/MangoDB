package store.net;

import store.MangoDB;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;

public class TCPServer {

    private final int port = 8080;
    private final MangoDB mangoDB = new MangoDB();

    public TCPServer() throws IOException {
    }

    public void start() {
        try {
            ServerSocket serverSocket = new ServerSocket(port);

            while (true) {
                final Socket socketClient = serverSocket.accept();
                handleClient(socketClient);
            }
        } catch (final IOException e) {
            System.out.println("Error starting server: " + e.getMessage());
            throw new RuntimeException(e);
        }
    }

    public void handleClient(final Socket socketClient) {
        System.out.println("Handling client: " + socketClient.getRemoteSocketAddress());

        try {
           final BufferedReader in = new BufferedReader(new InputStreamReader(socketClient.getInputStream()));
           final PrintWriter out = new PrintWriter(socketClient.getOutputStream(), true);

           String line;
           while ((line = in.readLine()) != null) {
               System.out.println("Received: " + line);
               out.println(mangoDB.handle(line));
           }

           socketClient.close();
        } catch (final IOException e) {
            System.out.println("Error handling client: " + e.getMessage());
            throw new RuntimeException(e);
        }
    }

    public static void main(String[] args) throws IOException {
        System.out.println("Starting server...!");
        final TCPServer server = new TCPServer();
        server.start();
    }
}
