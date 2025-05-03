package tree;

import client.MangoClient;
import commands.Command;
import commands.CommandParser;
import commands.CommandType;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static server.CommandProcessor.WRAP_GREEN;
import static server.CommandProcessor.WRAP_RED;

enum ServerStatus {
   ALIVE,
   DEAD,
   UNKNOWN;
}


class ServerInfo {
    private String host;
    private int port;
    private ServerStatus status;
    private ServerRole role;
    private MangoClient client;

    public ServerInfo(
           final String host,
           final int port,
           final ServerRole role
    ) {
        this.host = host;
        this.port = port;
        this.role = role;
        this.status = ServerStatus.ALIVE;
        this.client = new MangoClient(host, port);

        client.connect();
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public MangoClient getClient() {
        return client;
    }
}

public class MangoTree {
    private static String treebanner = """
             __  __                       _____             \s
            |  \\/  | __ _ _ __   __ _  __|_   _| __ ___  ___\s
            | |\\/| |/ _` | '_ \\ / _` |/ _ \\| || '__/ _ \\/ _ \\
            | |  | | (_| | | | | (_| | (_) | || | |  __/  __/
            |_|  |_|\\__,_|_| |_|\\__, |\\___/|_||_|  \\___|\\___|
                                |___/                       \s
         """;
    private int port;
    private ExecutorService healthCheckExecutor;
    private ExecutorService connectionExecutor;
    private HashMap<String, ServerInfo> registeredServers;
    private String primaryServerId;
    private List<String> secondaryServerIds;

    private List<CommandType> PRIMARY_COMMANDS = List.of(
            CommandType.PUT,
            CommandType.DELETE
    );

    private List<CommandType> SECONDARY_COMMANDS = List.of(
            CommandType.GET
    );

    private List<CommandType> TREE_COMMANDS = List.of(
            CommandType.REGISTER,
            CommandType.HEARTBEAT,
            CommandType.SECONDARIES
    );

    public MangoTree(int port) {
        this.port = port;
        this.connectionExecutor = Executors.newCachedThreadPool();
        this.healthCheckExecutor = Executors.newSingleThreadExecutor();
        this.registeredServers = new HashMap<>();
        this.secondaryServerIds = new ArrayList<>();
    }

    public void start() {
        System.out.println("Starting Mango tree server on port " + port);

        try (ServerSocket serverSocket = new ServerSocket(port)) {
            // if thread gets interrupted, stop everything
            // it will get interrupted during shutdown
            // but when does it really happen? we'll find out
            while (!Thread.currentThread().isInterrupted()) {
                final Socket socketClient = serverSocket.accept();
                connectionExecutor.submit(() -> handleClientConnection(socketClient));
            }
        } catch (IOException e) {
            System.err.println("Error serving socket connections: " + e.getMessage());
            throw new RuntimeException(e);
        } finally {
            stop();
        }
    }

    private void handleClientConnection(final Socket clientSocket) {
       System.out.println("Handling client connection from " + clientSocket.getRemoteSocketAddress());
       try(
           BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
           PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);
       ) {
           String line;
           while ((line = in.readLine()) != null) {;
               String response = processCommand(line);
               System.out.println("Received: " + line);
               out.println(response);
           }
       } catch (Exception e) {
           System.err.println("Error handling client connection: " + e.getMessage());
           throw new RuntimeException(e);
       } finally {
           try {
               System.out.println("Closing client connection from " + clientSocket.getRemoteSocketAddress());
               clientSocket.close();
           } catch (IOException e) {
               // ignore
           }
       }
    }

    private String processCommand(final String line) {
        final Command command = CommandParser.parse(line);

        assert command != null;

        if (PRIMARY_COMMANDS.contains(command.type())) {
            // send message to primary
            // if it's not there, say primary not registered

            if (primaryServerId == null) {
                return String.format(WRAP_RED, "PRIMARY NOT REGISTERED");
            }

            final ServerInfo primaryServer = registeredServers.get(primaryServerId);
            final String response = sendCommandToServer(primaryServer, command);
            System.out.println("Response from primary: " + response);

            return response;
        } else if (SECONDARY_COMMANDS.contains(command.type())) {
            int randomIndex = new Random().nextInt(secondaryServerIds.size());
            final String secondaryServerId = secondaryServerIds.get(randomIndex);

            System.out.println("Sending command to secondary server: " + secondaryServerId + " at index: " + randomIndex);
            final ServerInfo secondaryServer = registeredServers.get(secondaryServerId);
            return  sendCommandToServer(secondaryServer, command);
        } else if (command.type().equals(CommandType.REGISTER)) {
            System.out.println("Registering command: " + command);
            String role = command.args()[0];
            Integer port = Integer.valueOf(command.args()[1]);
            String host = command.args()[2];

            try {
                handleRegistration(role, port, host);
            } catch (final Exception e) {
                System.err.println("Error registering command: " + e.getMessage());
                return String.format(WRAP_RED, "ERROR REGISTERING: " + e.getMessage());
            }
        } else if (command.type().equals(CommandType.SECONDARIES)) {
            String response = "";
            for (final String secondaryServerId : secondaryServerIds) {
                final ServerInfo secondaryServer = registeredServers.get(secondaryServerId);
                response += secondaryServer.getHost() + " " + secondaryServer.getPort() + " ";
            }
            System.out.println("Response: " + response);
            return response;
        } else {
            System.err.println("Unknown command type: " + command.type());
            throw new RuntimeException("Unknown command type: " + command.type());
        }

        return String.format(WRAP_GREEN, "SUCCESSFULLY REGISTERED");
    }

    private String sendCommandToServer(final ServerInfo server, final Command command) {
        String response = "MISTAKE";
        try {
            if (command.type().equals(CommandType.PUT)) {
                String key = command.args()[0];
                String value = command.args()[1];

                server.getClient().put(key, value);
                response = String.format(WRAP_GREEN, "PUT SUCCESSFUL");
            } else if (command.type().equals(CommandType.GET)) {
                String key = command.args()[0];
                response = server.getClient().get(key);
            }

        } catch (final IOException e) {
            System.err.println("Error sending command: " + e.getMessage());
            throw new RuntimeException(e);
        }

        return response;
    }

    private void handleRegistration(final String roleStr, final Integer port, final String host) {
        ServerRole role = ServerRole.valueOf(roleStr.toUpperCase());
        String serverId =  String.format("%s:%d", role, port);

        if (role == ServerRole.PRIMARY) {
            if (primaryServerId != null && primaryServerId != serverId) {
                throw new RuntimeException("There's already a primary server registered");
            }
        }

        System.out.println("Registering " + role + " at " + host + ":" + port);

        if (registeredServers.containsKey(serverId)) {
            System.out.println("Server already registered");
        } else {
            registeredServers.put(serverId, new ServerInfo(host, port, role));
            if (role == ServerRole.PRIMARY) {
                primaryServerId = serverId;
            } else if (role == ServerRole.SECONDARY) {
                secondaryServerIds.add(serverId);
            }
        }

        System.out.println("Registered " + role + " at " + host + ":" + port);
    }

    public void stop() {
        // stop health check executor
        // stop connection executor
    }

    public static void main(String [] args) {
        int port = 9090;
        MangoTree tree = new MangoTree(port);
        tree.start();
    }
}

