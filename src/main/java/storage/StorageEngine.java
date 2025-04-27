package storage;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

public interface StorageEngine {
    CompletableFuture<Void> write(String key, String value);
    CompletableFuture<String> read(String key, String value);
    CompletableFuture<Void> delete(String key);
    CompletableFuture<Void> flush();
    String getStatus();
}
