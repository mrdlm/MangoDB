package storage;

import java.util.concurrent.CompletableFuture;

public record AsyncWriteRequest(String key, String value, CompletableFuture<Void> future) {
}
