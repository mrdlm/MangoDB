package storage;

import java.util.concurrent.CompletableFuture;

public interface StorageEngine {
    CompletableFuture<Void> write(String key, String value);

    CompletableFuture<String> read(String key);

    CompletableFuture<Void> delete(String key);

    CompletableFuture<Void> flush();

    StorageStatus getStatus();

    CompletableFuture<Boolean> exists(String key);
}
