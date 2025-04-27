package storage;


import storage.disk.DiskStore;

import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class AsyncDiskStore {
    private final BlockingQueue<AsyncWriteRequest> writeQueue;
    private final ExecutorService writeWorkerThreadPool;
    private int writeWorkersThreadCount;

    public AsyncDiskStore() throws IOException {
        super();
        writeQueue = new ArrayBlockingQueue<AsyncWriteRequest>(10000);
        writeWorkersThreadCount = 10;
        writeWorkerThreadPool = Executors.newFixedThreadPool(writeWorkersThreadCount);
        startWriteWorkers();
    }

    private void startWriteWorkers() {


    }

    private void processWriteQueue() {

    }


}
