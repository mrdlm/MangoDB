package storage;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

public class MultiThreadedStorageEngine implements StorageEngine {
    @Override
    public CompletableFuture<Void> write(String key, String value) {
        return null;
    }

    @Override
    public CompletableFuture<String> read(String key, String value) {
        return null;
    }

    @Override
    public CompletableFuture<Void> delete(String key) {
        return null;
    }

    @Override
    public CompletableFuture<Void> flush() {
        return null;
    }

    @Override
    public String getStatus() {
        return "";
    }
}
