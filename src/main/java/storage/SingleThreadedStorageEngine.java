package storage;

import config.ConfigManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storage.disk.DiskStore;
import storage.disk.SerialDiskStore;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;


public class SingleThreadedStorageEngine implements StorageEngine {
    private static final Logger logger = LoggerFactory.getLogger(SingleThreadedStorageEngine.class);
    private static final int MAX_BATCH_SIZE = 1000;
    private final DiskStore diskStore;
    private final MemStore memStore;
    private final FileChannel writeChannel;
    private final Thread writeThread; // Keep a reference to the thread
    private final BlockingQueue<AsyncWriteRequest> writeQueue;
    private Map<String, FileChannel> fileNamesToReadChannels;
    private volatile boolean running = true;

    public SingleThreadedStorageEngine() throws IOException {
        this.diskStore = new SerialDiskStore();
        this.memStore = new MemStore();

        writeQueue = new ArrayBlockingQueue<AsyncWriteRequest>(1000000);
        ConfigManager configManager = new ConfigManager("config.properties");
        writeChannel = getWriteFileChannel(configManager.getProperty("datapath"));
        fileNamesToReadChannels = getReadChannels(configManager.getProperty("datapath"));

        // a single thread will read from the queue and write to disk
        writeThread = new Thread(this::processWriteQueue, "storage-write-thread");
        writeThread.start();

        logger.info("Storage engine initialized. Write thread started.");
    }

    private Map<String, FileChannel> getReadChannels(final String datapath) {
    }

    private void addShutdownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutdown hook triggered for storage engine. Stopping write thread...");
            stopProcessing();
            try {
                // Wait a short time for the thread to finish
                writeThread.join(5000); // Wait up to 5 seconds
                if (writeThread.isAlive()) {
                    logger.warn("Write thread did not terminate gracefully after 5 seconds.");
                }
                // Close the write channel on shutdown
                if (writeChannel != null && writeChannel.isOpen()) {
                    writeChannel.close();
                    logger.info("Write channel closed.");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.error("Interrupted while waiting for write thread to stop.", e);
            } catch (IOException e) {
                logger.error("Error closing write channel during shutdown.", e);
            }
        }, "storage-shutdown-hook"));
    }

    public void stopProcessing() {
        this.running = false;
        // Interrupt the thread in case it's blocked in writeQueue.take()
        if (writeThread != null) {
            writeThread.interrupt();
        }
    }

    private void processWriteQueue() {
        while (running && !Thread.currentThread().isInterrupted()) {
            try {
                final List<AsyncWriteRequest> batch = new ArrayList<>(MAX_BATCH_SIZE);

                // logger.info("Queue size:" + writeQueue.size());
                final AsyncWriteRequest writeRequest = writeQueue.take();
                batch.add(writeRequest);

                writeQueue.drainTo(batch, MAX_BATCH_SIZE - 1);
                // we don't need to send future to diskStore, but
                // if we remove future from each request, it's going
                // waste some time
                // logger.info("Batch size:" + batch.size());
                diskStore.write(batch, writeChannel);

                for (final AsyncWriteRequest request : batch) {
                    request.future().complete(null);
                }
            } catch (InterruptedException e) {
                logger.error("Error processing write queue", e);
                break;
            }
        }

        stopProcessing();
    }

    private FileChannel getWriteFileChannel(final String datapath) throws IOException {
        final File dir = new File(datapath);
        final File[] files = dir.listFiles();

        if (files != null && files.length > 0) {
            Optional<File> latestFile = Arrays.stream(files).max(Comparator.comparing(File::getName));
            return FileChannel.open(
                    Path.of(datapath + latestFile.get().getName()),
                    StandardOpenOption.CREATE,
                    StandardOpenOption.APPEND
            );
        } else {
            return FileChannel.open(
                    Path.of(datapath + String.valueOf(System.currentTimeMillis()) + ".data"),
                    StandardOpenOption.CREATE,
                    StandardOpenOption.APPEND
            );
        }
    }

    @Override
    public CompletableFuture<Void> write(String key, String value) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        AsyncWriteRequest asyncWriteRequest = new AsyncWriteRequest(key, value, System.currentTimeMillis(), future);

        if (!writeQueue.offer(asyncWriteRequest)) {
            throw new RuntimeException("Write queue is full");
        }

        return future;
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
