package store.engine;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static store.engine.LogWriter.TOMBSTONE_VALUE;

public class StorageService {
    public static final String ANSI_RESET = "\u001B[0m";
    public static final String ANSI_GREEN = "\u001B[32m";
    public static final String ANSI_YELLOW = "\u001B[33m";
    public static final String ANSI_RED = "\u001B[31m";
    public static final String ANSI_CYAN = "\u001B[36m";

    public static final String RESPONSE_INVALID_INPUT = "INVALID INPUT";
    private static final String RESPONSE_NOT_FOUND = "NOT FOUND";
    public static final String RESERVED_KEYWORD_TOMBSTONE = "RESERVED KEYWORD __TOMBSTONE__";
    private static final String CMD_STATUS = "STATUS";
    private static final String CMD_HELP = "HELP";
    private final StorageEngine storageEngine;
    private static long startTime;

    private static final String RESPONSE_EMPTY_INPUT = "";
    private static final String CMD_PUT = "PUT";
    private static final String CMD_GET = "GET";
    private static final String CMD_DELETE = "DELETE";
    private static final String CMD_FLUSH = "FLUSH";
    private static final String CMD_EXISTS = "EXISTS";

    public StorageService() throws IOException {
        storageEngine = new StorageEngine();
        startTime = System.currentTimeMillis();
    }

    public CompletableFuture<String> handle(final String input) throws IOException, ExecutionException, InterruptedException {
       if (Objects.isNull(input) || input.isEmpty()) {
           return CompletableFuture.completedFuture(RESPONSE_EMPTY_INPUT);
       }

       int firstSpaceIndex = input.indexOf(" ");
       String command;
       String argsString = "";

       if (firstSpaceIndex == -1) {
           command = input.toUpperCase();
       } else {
           command = input.substring(0, firstSpaceIndex).toUpperCase();
           argsString = input.substring(firstSpaceIndex + 1).strip();
       }

       System.out.println("Received command: " + command);
        return switch (command) {
            case CMD_PUT -> handlePut(argsString);
            case CMD_GET -> handleGet(argsString);
            case CMD_DELETE -> handleDelete(argsString);
            case CMD_FLUSH -> handleFlush();
            case CMD_EXISTS -> handleExists(argsString);
            case CMD_STATUS -> handleStatus();
            case CMD_HELP -> handleHelp();
            default -> CompletableFuture.completedFuture(ANSI_RED + RESPONSE_INVALID_INPUT + "\n" + ANSI_RESET);
        };
    }

    private CompletableFuture<String> handleStatus() {
        final long timeFromStartSeconds = (System.currentTimeMillis() - startTime) / 1000;
        final int keyDirSize = storageEngine.getKeyDirSize();
        final int diskSize = storageEngine.getDiskSize();
        final int dataFilesCount = storageEngine.getDataFilesCount();

        final String status = String.format(ANSI_CYAN + """
                🌟 MangoDB Node Status
                ─────────────────────────────
                Disk Size:           %.2f MB
                Data Files:          %d
                KeyDir Entries:      %d
                Uptime:              %d seconds
                \n""" + ANSI_RESET, diskSize / (1024.0 * 1024.0), dataFilesCount, keyDirSize, timeFromStartSeconds
        );

        return CompletableFuture.completedFuture(status);
    }

    private CompletableFuture<String> handleExists(final String argsString) {
        final String key = argsString.strip();

        if (key.isEmpty()) {
            return CompletableFuture.completedFuture(ANSI_RED + RESPONSE_INVALID_INPUT + " (Usage: EXISTS <key>)\n" + ANSI_RESET);
        }

        final Boolean result = storageEngine.exists(key);
        return CompletableFuture.completedFuture(ANSI_YELLOW + result + "\n" + ANSI_RESET);
    }

    private CompletableFuture<String> handleFlush() throws IOException, ExecutionException, InterruptedException {
        return storageEngine.flush();
    }

    private CompletableFuture<String> handleHelp() throws IOException, ExecutionException, InterruptedException {
        return CompletableFuture.completedFuture(ANSI_CYAN + """
        Available Commands:
        - PUT <key> <value>
        - GET <key>
        - DELETE <key>
        - EXISTS <key>
        - FLUSH
        - STATUS
        - HELP\n
        """ + ANSI_RESET);
    }

    private CompletableFuture<String> handleDelete(String argsString) {
        final String key = argsString.strip();

        if (key.isEmpty()) {
            return CompletableFuture.completedFuture(ANSI_RED + RESPONSE_INVALID_INPUT + " (Usage: DELETE <key>)\n" + ANSI_RESET);
        }

        if (!storageEngine.exists(key)) {
           return CompletableFuture.completedFuture(ANSI_YELLOW + RESPONSE_NOT_FOUND + ANSI_RESET);
        }

        try {
            return storageEngine.delete(key);
        } catch (final IOException e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    private CompletableFuture<String> handleGet(final String argsString) {
        final String key = argsString.strip();

        if (key.isEmpty()) {
            return CompletableFuture.completedFuture(ANSI_RED + RESPONSE_INVALID_INPUT + " (Usage: GET <key>)\n" + ANSI_RESET);
        }

        try {
            final String value = storageEngine.read(key);

            if (value == null) {
                return CompletableFuture.completedFuture(ANSI_YELLOW + RESPONSE_NOT_FOUND + "\n" + ANSI_RESET);
            }

            return CompletableFuture.completedFuture(ANSI_YELLOW + value + "\n" + ANSI_RESET);
        } catch (final IOException e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    private CompletableFuture<String> handlePut(final String argsString) {
        if (argsString.isEmpty()) {
            return CompletableFuture.completedFuture(ANSI_RED + RESPONSE_INVALID_INPUT + " (Usage: PUT <key> <value>)\n" + ANSI_RESET);
        }

        int firstSpaceIndex = argsString.indexOf(" ");

        if (firstSpaceIndex == -1 || firstSpaceIndex == argsString.length() - 1) {
            return CompletableFuture.completedFuture(ANSI_RED + RESPONSE_INVALID_INPUT + " (Usage: PUT <key> <value>)\n" + ANSI_RESET);
        }

        final String key = argsString.substring(0, firstSpaceIndex);
        final String value = argsString.substring(firstSpaceIndex + 1).strip();

        if (value.equals(TOMBSTONE_VALUE)) {
            return CompletableFuture.completedFuture(ANSI_RED + RESERVED_KEYWORD_TOMBSTONE + "\n" + ANSI_RESET);
        }

        return storageEngine.writeToQueue(key, value);
    }
}
