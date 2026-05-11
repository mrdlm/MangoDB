package storage;

import java.util.concurrent.CompletableFuture;

public class MultiThreadedStorageEngine implements StorageEngine {
    @Override
    public CompletableFuture<Void> write(String key, String value) {
        return null;
    }

    @Override
    public CompletableFuture<String> read(String key) {
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
    public StorageStatus getStatus() {
        return new StorageStatus(0, 0, 0, 0);
    }

    @Override
    public CompletableFuture<Boolean> exists(String key) {
        return null;
    }
}
