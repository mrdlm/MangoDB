package storage;

import java.util.concurrent.CompletableFuture;

public record AsyncWriteRequest(String key, String value, long timestamp, CompletableFuture<Void> future) {
}
