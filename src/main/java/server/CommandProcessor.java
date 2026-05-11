package server;

import commands.Command;
import commands.CommandParser;
import config.ConfigManager;
import jdk.jshell.SourceCodeAnalysis;
import replication.ReplicationManager;
import storage.MultiThreadedStorageEngine;
import storage.SingleThreadedStorageEngine;
import storage.StorageEngine;
import storage.StorageStatus;
import tree.ServerRole;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

public class CommandProcessor {

    public static final String ANSI_RESET = "\u001B[0m";
    public static final String ANSI_GREEN = "\u001B[32m";
    public static final String ANSI_YELLOW = "\u001B[33m";
    public static final String ANSI_RED = "\u001B[31m";
    public static final String ANSI_CYAN = "\u001B[36m";

    public static final String WRAP_GREEN = ANSI_GREEN + "%s" + ANSI_RESET;
    public static final String WRAP_RED = ANSI_RED + "%s" + ANSI_RESET;
    public static final String WRAP_CYAN = ANSI_CYAN + "%s" + ANSI_RESET;
    public static final String WRAP_YELLOW = ANSI_YELLOW + "%s" + ANSI_RESET;

    public static final String RESPONSE_INVALID_INPUT = "INVALID INPUT";
    public static final String RESPONSE_OK = "OK";
    private static final String RESPONSE_NOT_FOUND = "NOT FOUND";
    public static final String RESERVED_KEYWORD_TOMBSTONE = "RESERVED KEYWORD __TOMBSTONE__";
    private static final String CMD_STATUS = "STATUS";
    private static final String CMD_HELP = "HELP";
    private static long startTime;

    private static final String RESPONSE_EMPTY_INPUT = "";
    private static final String CMD_PUT = "PUT";
    private static final String CMD_GET = "GET";
    private static final String CMD_DELETE = "DELETE";
    private static final String CMD_FLUSH = "FLUSH";
    private static final String CMD_EXISTS = "EXISTS";

    private StorageEngine storageEngine;
    private ReplicationManager replicationManager;
    private ServerRole serverRole = ServerRole.PRIMARY;

    private boolean returnKeysOnWrite;

    public CommandProcessor() throws IOException {

        ConfigManager configManager = new ConfigManager("config.properties");
        String engineType = configManager.getProperty("storage.type");
        returnKeysOnWrite = configManager.getBooleanProperty("return.key.on.writes", false);
        switch (engineType) {
            case "single" -> storageEngine = new SingleThreadedStorageEngine();
            case "multi" -> storageEngine = new MultiThreadedStorageEngine();
            default -> throw new UnsupportedOperationException("Unsupported storage engine type specified");
        }
        // it should start replication manager only when it's primary
        boolean replicationEnabled = configManager.getBooleanProperty("replication.enabled", false);
        replicationManager = new ReplicationManager(replicationEnabled);
    }

    public CommandProcessor(final ServerRole role) throws IOException {
        this();
        this.serverRole = role;
        ConfigManager configManager = new ConfigManager("config.properties");
        boolean replicationEnabled = configManager.getBooleanProperty("replication.enabled", false);
        replicationManager = new ReplicationManager(replicationEnabled);
    }

    public ServerRole getRole() {
        return serverRole;
    }

    CompletableFuture<String> process(final String input) {
        try {
            final Command command = CommandParser.parse(input);

            if (command == null) {
                return CompletableFuture.completedFuture(String.format(WRAP_RED, "ERROR: Unable to parse command"));
            }

            return switch (command.type()) {
                case PUT -> handlePut(command.args());
                case GET -> handleGet(command.args());
                case DELETE -> handleDelete(command.args());
                case FLUSH -> handleFlush(command.args());
                case EXISTS -> handleExists(command.args());
                case STATUS -> handleStatus(command.args());

                default ->
                    CompletableFuture.completedFuture(String.format(WRAP_RED, "ERROR: " + RESPONSE_INVALID_INPUT));
            };
        } catch (final Exception e) {
            return CompletableFuture.completedFuture(String.format(WRAP_RED, "ERROR: " + e.getMessage()));
        }
    }

    private CompletableFuture<String> handleExists(String[] args) {
        final CompletableFuture<Boolean> existsFuture = storageEngine.exists(args[0]);

        return existsFuture.thenApply(exists -> {
            if (exists) {
                return String.format(WRAP_GREEN, "EXISTS");
            } else {
                return String.format(WRAP_YELLOW, "DOES NOT EXIST");
            }
        });
    }

    private CompletableFuture<String> handleFlush(String[] args) {
        final CompletableFuture<Void> flushFuture = storageEngine.flush();

        return flushFuture.thenApply(voidResult -> {
            return String.format(WRAP_GREEN, "SUCCESS");
        });
    }

    private CompletableFuture<String> handlePut(final String[] args) {
        final CompletableFuture<Void> responseFuture = storageEngine.write(args[0], args[1]);

        return responseFuture.thenApply(voidResult -> {
            if (serverRole == ServerRole.PRIMARY && replicationManager.isReplicationEnabled()) {
                System.out.println("Replicating to secondaries");
                replicationManager.asyncReplicate(args[0], args[1]);
            }

            if (returnKeysOnWrite) {
                return args[0];
            } else {
                return String.format(WRAP_GREEN, RESPONSE_OK);
            }
        }).exceptionally(e -> String.format(WRAP_RED, "ERROR: " + e.getMessage()));
    }

    private CompletableFuture<String> handleGet(final String[] args) {
        final CompletableFuture<String> readFuture = storageEngine.read(args[0]);

        return readFuture.thenApply(value -> {
            if (value == null) {
                System.out.println(RESPONSE_NOT_FOUND);
                return String.format(WRAP_RED, RESPONSE_NOT_FOUND);
            }

            return String.format(WRAP_YELLOW, value);
        });
    }

    private CompletableFuture<String> handleDelete(final String[] args) {
        final CompletableFuture<Void> deleteFuture = storageEngine.delete(args[0]);

        return deleteFuture.thenApply(voidResult -> {
            return String.format(WRAP_GREEN, "SUCCESS");
        });
    }

    private CompletableFuture<String> handleStatus(final String[] args) {
        final StorageStatus status = storageEngine.getStatus();
        final String body = String.format(
                "Disk Size:    %s%n" +
                "File Count:   %d%n" +
                "Keys:         %d%n" +
                "Uptime:       %s",
                formatBytes(status.diskSizeBytes()),
                status.fileCount(),
                status.keyCount(),
                formatDuration(status.uptimeMillis()));
        return CompletableFuture.completedFuture(String.format(WRAP_CYAN, body));
    }

    private static String formatBytes(final long bytes) {
        if (bytes < 1024) {
            return bytes + " B";
        }
        final String[] units = {"KB", "MB", "GB", "TB"};
        double size = bytes / 1024.0;
        int unitIndex = 0;
        while (size >= 1024 && unitIndex < units.length - 1) {
            size /= 1024;
            unitIndex++;
        }
        return String.format("%.2f %s", size, units[unitIndex]);
    }

    private static String formatDuration(final long millis) {
        long totalSeconds = millis / 1000;
        long days = totalSeconds / 86400;
        long hours = (totalSeconds % 86400) / 3600;
        long minutes = (totalSeconds % 3600) / 60;
        long seconds = totalSeconds % 60;

        StringBuilder sb = new StringBuilder();
        if (days > 0) sb.append(days).append("d ");
        if (hours > 0 || sb.length() > 0) sb.append(hours).append("h ");
        if (minutes > 0 || sb.length() > 0) sb.append(minutes).append("m ");
        sb.append(seconds).append("s");
        return sb.toString();
    }
}
