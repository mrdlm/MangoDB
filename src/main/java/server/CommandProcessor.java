package server;

import commands.Command;
import commands.CommandParser;
import config.ConfigManager;
import storage.MultiThreadedStorageEngine;
import storage.SingleThreadedStorageEngine;
import storage.StorageEngine;

import java.util.concurrent.CompletableFuture;

public class CommandProcessor {

    private StorageEngine storageEngine;
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
    private static long startTime;

    private static final String RESPONSE_EMPTY_INPUT = "";
    private static final String CMD_PUT = "PUT";
    private static final String CMD_GET = "GET";
    private static final String CMD_DELETE = "DELETE";
    private static final String CMD_FLUSH = "FLUSH";
    private static final String CMD_EXISTS = "EXISTS";

    public CommandProcessor() {

        ConfigManager configManager = new ConfigManager("config.properties");
        String engineType = configManager.getProperty("storage.type");

        switch (engineType) {
            case "single" -> storageEngine = new SingleThreadedStorageEngine();
            case "multi" -> storageEngine = new MultiThreadedStorageEngine();
            default -> throw new UnsupportedOperationException("Unsupported storage engine type specified");
        }
    }

    CompletableFuture<String> process(final String input) {
        final Command command = CommandParser.parse(input);

        assert command != null;
        return switch (command.type()) {
            case PUT -> handlePut(command.args());
            case GET -> handleGet(command.args());
            default -> CompletableFuture.completedFuture(RESPONSE_INVALID_INPUT);
        };
    }

    private CompletableFuture<String> handlePut(String[] args) {
        return CompletableFuture.completedFuture("OK");
    }

    private CompletableFuture<String> handleGet(String[] args) {
        return CompletableFuture.completedFuture("OK");
    }
}
