package store;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

public class MangoDB {
    public static final String RESPONSE_INVALID_INPUT = "INVALID INPUT";
    private static final String RESPONSE_NOT_FOUND = "NOT_FOUND";
    private final StorageEngine storageEngine;

    private static final String RESPONSE_EMPTY_INPUT = "";
    private static final String CMD_PUT = "PUT";
    private static final String CMD_GET = "GET";
    private static final String CMD_DELETE = "DELETE";
    private static final String CMD_FLUSH = "FLUSH";
    private static final String CMD_EXISTS = "EXISTS";

    public MangoDB() throws IOException {
        storageEngine = new StorageEngine();
    }

    public CompletableFuture<String> handle(final String input) {
       if (Objects.isNull(input) || input.isEmpty()) {
           return CompletableFuture.completedFuture(RESPONSE_EMPTY_INPUT);
       }

       int firstSpaceIndex = input.indexOf(" ");
       String command;
       String argsString;

       if (firstSpaceIndex == -1) {
           command = input.toUpperCase();
           return CompletableFuture.completedFuture(RESPONSE_INVALID_INPUT);
       } else {
           command = input.substring(0, firstSpaceIndex).toUpperCase();
           argsString = input.substring(firstSpaceIndex + 1).strip();
       }

        return switch (command) {
            case CMD_PUT -> handlePut(argsString);
            case CMD_GET -> handleGet(argsString);
            case CMD_DELETE -> handleDelete(argsString);
            case CMD_FLUSH -> handleFlush();
            case CMD_EXISTS -> handleExists(argsString);
            default -> CompletableFuture.completedFuture(RESPONSE_INVALID_INPUT);
        };
    }

    private CompletableFuture<String> handleExists(final String argsString) {
        final String key = argsString.strip();

        if (key.isEmpty()) {
            return CompletableFuture.completedFuture(RESPONSE_INVALID_INPUT + " (Usage: EXISTS <key>)");
        }

        final Boolean result = storageEngine.exists(key);
        return CompletableFuture.completedFuture(String.valueOf(result));
    }

    private CompletableFuture<String> handleFlush() {
        return CompletableFuture.completedFuture(RESPONSE_INVALID_INPUT);
    }

    private CompletableFuture<String> handleDelete(String argsString) {
        return CompletableFuture.completedFuture(RESPONSE_INVALID_INPUT);
    }

    private CompletableFuture<String> handleGet(final String argsString) {
        final String key = argsString.strip();

        if (key.isEmpty()) {
            return CompletableFuture.completedFuture(RESPONSE_INVALID_INPUT + " (Usage: GET <key>)");
        }

        try {
            final String value = storageEngine.read(key);
            return CompletableFuture.completedFuture(Objects.requireNonNullElse(value, RESPONSE_NOT_FOUND));
        } catch (final IOException e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    private CompletableFuture<String> handlePut(final String argsString) {
        int firstSpaceIndex = argsString.indexOf(" ");

        if (firstSpaceIndex == -1 || firstSpaceIndex == argsString.length() - 1) {
            return CompletableFuture.completedFuture(RESPONSE_INVALID_INPUT + " (Usage: PUT <key> <value>)");
        }

        final String key = argsString.substring(0, firstSpaceIndex).toUpperCase();
        final String value = argsString.substring(firstSpaceIndex + 1).strip();

        return storageEngine.writeToQueue(key, value);
    }
}
