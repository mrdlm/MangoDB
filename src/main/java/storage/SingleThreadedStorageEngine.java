package storage;

import config.ConfigManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storage.disk.DiskStore;
import storage.disk.SerialDiskStore;
import storage.disk.WriteResult;
import storage.mem.MemRecord;
import storage.mem.MemStore;
import storage.mem.UnsafeMemStore;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;

public class SingleThreadedStorageEngine implements StorageEngine {
    private static final Logger logger = LoggerFactory.getLogger(SingleThreadedStorageEngine.class);
    private static final int MAX_BATCH_SIZE = 1000;
    private static final int MAX_WRITE_CHANNEL_SIZE = 64 * 1024 * 1024; // 64 MB
    private final DiskStore diskStore;
    private final MemStore memStore;
    private final String DATA_PATH;

    private final Thread writeThread; // Keep a reference to the thread
    private final BlockingQueue<AsyncWriteRequest> writeQueue;
    private final Map<String, FileChannel> fileNamesToReadChannels;
    private volatile boolean running = true;

    String currentWriteFileName;
    FileChannel writeChannel;

    public SingleThreadedStorageEngine() throws IOException {
        this.diskStore = new SerialDiskStore();
        this.memStore = new UnsafeMemStore();

        currentWriteFileName = "";
        writeQueue = new ArrayBlockingQueue<>(1000000);
        ConfigManager configManager = new ConfigManager("config.properties");
        DATA_PATH = configManager.getProperty("datapath");

        writeChannel = getWriteFileChannel();
        fileNamesToReadChannels = new HashMap<>();
        constructReadChannelsMap();

        // a single thread will read from the queue and write to disk
        writeThread = new Thread(this::processWriteQueue, "storage-write-thread");
        writeThread.start();

        logger.info("Storage engine initialized. Write thread started.");
        addShutdownHook();
    }

    @Override
    public CompletableFuture<Void> write(String key, String value) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        AsyncWriteRequest asyncWriteRequest = new AsyncWriteRequest(key, value, future);

        if (!writeQueue.offer(asyncWriteRequest)) {
            throw new RuntimeException("Write queue is full");
        }

        return future;
    }

    @Override
    public CompletableFuture<String> read(final String key) {
        final Optional<MemRecord> memRecord = memStore.read(key);

        if (memRecord.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }

        try {
            final String value = diskStore.read(memRecord.get().valueOffset(), fileNamesToReadChannels.get(memRecord.get().filename()));
            return CompletableFuture.completedFuture(value);
        } catch (final IOException e) {
            logger.error("Exception while reading request", e);
            throw new RuntimeException(e);
        }
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

    private void processWriteQueue() {
        while (running && !Thread.currentThread().isInterrupted()) {
            try {
                final List<AsyncWriteRequest> batch = new ArrayList<>(MAX_BATCH_SIZE);

                // logger.info("Queue size:" + writeQueue.size());
                final AsyncWriteRequest writeRequest = writeQueue.take();
                batch.add(writeRequest);

                writeQueue.drainTo(batch, MAX_BATCH_SIZE - 1);
                final List<WriteResult> writeResults = diskStore.write(batch, writeChannel);

                for (int i = 0; i < writeResults.size(); i++) {
                    final AsyncWriteRequest batchItem = batch.get(i);
                    memStore.write(batchItem.key(), writeResults.get(i).offset(), currentWriteFileName, writeResults.get(i).timestamp());

                    // set value to null since the CompletableFuture is of void type
                    batchItem.future().complete(null);
                }

                if (writeChannel.position() > MAX_WRITE_CHANNEL_SIZE) {
                    updateCurrentWriteFile();
                }

            } catch (InterruptedException e) {
                logger.error("Error processing write queue", e);
                break;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        stopProcessing();
    }

    private void constructReadChannelsMap() throws IOException {
        final File dir = new File(DATA_PATH);
        final File[] files = dir.listFiles();

        assert files != null;
        for (final File file : files) {
            FileChannel readFileChannel = FileChannel.open(
                    Path.of(file.getAbsolutePath()),
                    StandardOpenOption.READ);
            fileNamesToReadChannels.put(file.getName(), readFileChannel);
        }
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

    private void stopProcessing() {
        this.running = false;
        // Interrupt the thread in case it's blocked in writeQueue.take()
        if (writeThread != null) {
            writeThread.interrupt();
        }
    }

    private void updateCurrentWriteFile() throws IOException {
        currentWriteFileName = System.currentTimeMillis() + ".data";
        try {
            writeChannel = FileChannel.open(
                    Path.of(DATA_PATH + currentWriteFileName),
                    StandardOpenOption.CREATE,
                    StandardOpenOption.APPEND
            );

            FileChannel currentReadFileChannel = FileChannel.open(
                    Path.of(DATA_PATH + currentWriteFileName),
                    StandardOpenOption.READ
            );
            fileNamesToReadChannels.put(currentWriteFileName, currentReadFileChannel);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private FileChannel getWriteFileChannel() throws IOException {
        final File dir = new File(DATA_PATH);
        final File[] files = dir.listFiles();

        if (files != null && files.length > 0) {
            Optional<File> latestFile = Arrays.stream(files).max(Comparator.comparing(File::getName));
            currentWriteFileName = latestFile.get().getName();
        } else {
            currentWriteFileName = System.currentTimeMillis() + ".data";
        }

        return FileChannel.open(
                Path.of(DATA_PATH + currentWriteFileName),
                StandardOpenOption.CREATE,
                StandardOpenOption.APPEND
        );
    }

}
