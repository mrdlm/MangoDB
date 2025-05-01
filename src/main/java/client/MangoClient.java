package client;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

public class MangoClient implements AutoCloseable {

    private final String host;
    private final int port;
    private Socket socket;
    private boolean connected = false;
    private BufferedReader in;
    private PrintWriter out;

    public MangoClient(final String host, final int port) {
        this.host = host;
        this.port = port;
    }

    public void connect() {
        if (connected) {
            throw new IllegalStateException("Already connected");
        }

        try {
            socket = new Socket(host, port);
            connected = true;

            // set out and in channels
            // this is just the standard way to create them
            out = new PrintWriter(socket.getOutputStream(), true);
            in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            System.out.println("Connected to " + host + ":" + port);
        } catch (IOException e) {
            System.err.println("Could not connect to " + host + ":" + port + ": " + e.getMessage());
            throw new RuntimeException(e);
        }
    }

    public void put(final String key, final String value) throws IOException {
        if (!connected) {
            connect();
        }

        out.println(String.format("put %s %s", key, value));
        String response = in.readLine();

        if (response.contains("ERROR")) {
            throw new IOException(response);
        }
    }

    public String get(final String key) throws IOException {
        if (!connected) {
            connect();
        }

        out.println(String.format("get %s", key));
        String response = in.readLine();

        if (response.contains("ERROR")) {
            throw new IOException(response);
        }

        return response;
    }

    @Override
    public void close() throws Exception {
        if (connected) {
            socket.close();
        }
    }

    public static void main(String[] args) throws InterruptedException, IOException {
        System.out.println("Running MangoClient...");
        MangoClient client = new MangoClient("localhost", 8080);
        client.connect();

        System.out.println(client.get("hey"));
        System.out.println(client.get("hdawey"));
        System.out.println(client.get("hdawey"));

        Thread.sleep(10000);
    }
}
