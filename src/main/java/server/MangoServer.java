package server;

import config.ConfigManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class MangoServer {

    private static final Logger logger = LoggerFactory.getLogger(MangoServer.class);
    private static final int SERVER_SOCKET_BACKLOG = 1000;

    private final int port;
    private final ExecutorService threadPool;
    private final int threadCount;
    private final CommandProcessor commandProcessor;
    private final boolean orderedResponse;

    private boolean running = true;


    public MangoServer() throws IOException {
        ConfigManager manager = new ConfigManager("config.properties");
        this.port = manager.getIntProperty("port", 8080);

        threadCount = manager.getIntProperty("server.threads", 1);
        this.threadPool = Executors.newFixedThreadPool(threadCount);

        this.orderedResponse = manager.getBooleanProperty("ordered.response", false);

        this.commandProcessor = new CommandProcessor();
        registerShutdownHook();
    }

    private void registerShutdownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutdown hook triggered!");
            shutdown();
        }));
    }

    private void shutdown() {
        running = false;
        threadPool.shutdown();
        logger.info("Shutting down MangoServer...");

        try {
            if (!threadPool.awaitTermination(10, TimeUnit.SECONDS)) {
                logger.info("Forcing shutdown...");
                threadPool.shutdownNow();
            }
        } catch (final InterruptedException e) {
            threadPool.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    public void start() {
        logger.info("MangoServer starting...");
        logger.info("Server thread count: {}", threadCount);
        logger.info("Ordered response: {}", orderedResponse);
        runServer();
    }

    private void runServer() {
        try (ServerSocket serverSocket = new ServerSocket(port, SERVER_SOCKET_BACKLOG)) {
            logger.info("MangoServer listening on port {}", port);

            while (running) {
                try {
                    // blocks until a client connects
                    final Socket socketClient = serverSocket.accept();
                    logger.info("Accepted client connection from {}", socketClient.getRemoteSocketAddress());

                    // asynchronously handle the client connection
                    threadPool.submit(() -> handleClient(socketClient));
                } catch (final IOException e) {
                    if (running) {
                        logger.error("Error accepting client connection: {}", e.getMessage());
                    }
                }
            }
        }  catch (final IOException e) {
            logger.error("Error handling client connection {}", e.getMessage());
            throw new RuntimeException("Error handling client connection", e);
        } finally {
            shutdown();
        }
    }

    public void handleClient(final Socket socketClient)  {
        logger.debug("Client at {} assigned to thread {}", socketClient.getRemoteSocketAddress(), Thread.currentThread().getName());
        try (socketClient) {
            final BufferedReader in = new BufferedReader(new InputStreamReader(socketClient.getInputStream()));
            final PrintWriter out = new PrintWriter(socketClient.getOutputStream(), true);

            String line;
            while ((line = in.readLine()) != null) {
                // logger.debug("received: {}", line);
                 final CompletableFuture<String> response = commandProcessor.process(line)
                         .thenApply(result -> {
                            out.println(result);
                            return result;
                        })
                        .exceptionally(e -> {
                            logger.error("Error processing command: {}", e.getMessage());
                            throw new RuntimeException(e);
                        });

                // If the server is configured to return ordered responses,
                // wait for the response to be sent before accepting the next command
                if (orderedResponse) {
                    response.join();
                    out.flush();
                }
            }

            logger.info("Client at {} disconnected", socketClient.getRemoteSocketAddress());
        } catch (final IOException e) {
            System.out.println("Error handling client: " + e.getMessage());
            throw new RuntimeException(e);
        } catch (final Exception e) {
            logger.error("Unknown exception happened: {}", e.getMessage());
            throw new RuntimeException(e);
        }
    }
}
