package server;

import commands.Command;
import commands.CommandParser;
import config.ConfigManager;
import jdk.jshell.SourceCodeAnalysis;
import storage.MultiThreadedStorageEngine;
import storage.SingleThreadedStorageEngine;
import storage.StorageEngine;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

public class CommandProcessor {

    private StorageEngine storageEngine;
    public static final String ANSI_RESET = "\u001B[0m";
    public static final String ANSI_GREEN = "\u001B[32m";
    public static final String ANSI_YELLOW = "\u001B[33m";
    public static final String ANSI_RED = "\u001B[31m";
    public static final String ANSI_CYAN = "\u001B[36m";

    public static final String WRAP_GREEN = ANSI_GREEN + "%s" + ANSI_RESET;
    public static final String WRAP_RED = ANSI_RED + "%s" + ANSI_RESET;
    public static final String WRAP_CYAN = ANSI_CYAN+ "%s" + ANSI_RESET;
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
                default -> CompletableFuture.completedFuture(String.format(WRAP_RED, "ERROR: " + RESPONSE_INVALID_INPUT));
            };
        } catch (final Exception e) {
            return CompletableFuture.completedFuture(String.format(WRAP_RED, "ERROR: " + e.getMessage()));
        }
    }

    private CompletableFuture<String> handlePut(final String[] args) {
        final CompletableFuture<Void> responseFuture = storageEngine.write(args[0], args[1]);

        return responseFuture.thenApply(voidResult -> {
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
                    return  String.format(WRAP_RED, RESPONSE_NOT_FOUND);
                }

                return String.format(WRAP_YELLOW, value);
        });
    }
}
