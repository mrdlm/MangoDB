package store.net;

import store.MangoDB;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class MangoDBServer {

    private final int port = 8080;
    private final MangoDB mangoDB = new MangoDB();
    private final ExecutorService threadPool;
    private final int numThreads;
    private volatile boolean running = true;

    public MangoDBServer() throws IOException {
        this.numThreads = Runtime.getRuntime().availableProcessors();
        this.threadPool = Executors.newFixedThreadPool(100); // 2 * numThreads);
        System.out.println("Number of available processors: " + numThreads);

        // so that forced shut-downs also close the threadpool
        registerShutdownHook();
    }

    private void registerShutdownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutdown hook triggered!");
            shutdown();
        }));
    }

    public void start() {
        try (ServerSocket serverSocket = new ServerSocket(port)) {
            System.out.println("Server listening on port " + port);

            while (running) {
                try {
                    final Socket socketClient = serverSocket.accept(); // blocking call
                    System.out.println("Accepted client connection from " + socketClient.getRemoteSocketAddress());
                    threadPool.submit(() -> handleClient(socketClient));
                } catch (final IOException e) {
                    if (running) {
                        System.out.println("Error accepting client connection: " + e.getMessage());
                    }
                }
            }
        } catch (IOException e) {
            System.out.println("Error starting server: " + e.getMessage());
            throw new RuntimeException(e);
        } finally {
            shutdown();
        }
    }

    private void shutdown() {
        running = false;

        if (threadPool != null) {
            threadPool.shutdown();
            System.out.println("Shutting down thread pool");

            try {
                if (!threadPool.awaitTermination(10, TimeUnit.SECONDS)) {
                    threadPool.shutdownNow();
                }
            } catch (final InterruptedException e) {
                threadPool.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
    }

    public void handleClient(final Socket socketClient) {
        // System.out.println("Handling client: " + socketClient.getRemoteSocketAddress());

        try {
           final BufferedReader in = new BufferedReader(new InputStreamReader(socketClient.getInputStream()));
           final PrintWriter out = new PrintWriter(socketClient.getOutputStream(), true);

           String line;
           while ((line = in.readLine()) != null) {
               // System.out.println("Received: " + line);
               mangoDB.handle(line)
                       .thenAccept(out::println)
                       .exceptionally(e -> {
                           System.out.println("Error handling client: " + e.getMessage());
                           return null;
                       });
           }

           socketClient.close();
        } catch (final IOException e) {
            System.out.println("Error handling client: " + e.getMessage());
            throw new RuntimeException(e);
        }
    }

    public static void main(String[] args) throws IOException {
        System.out.println("Starting server...!");
        final MangoDBServer server = new MangoDBServer();
        server.start();
    }
}
