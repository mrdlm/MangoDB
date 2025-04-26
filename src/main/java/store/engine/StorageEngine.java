package store.engine;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;

import static store.engine.LogWriter.FLUSH_TOMBSTONE_VALUE;
import static store.engine.LogWriter.TOMBSTONE_VALUE;
import static store.engine.StorageService.ANSI_GREEN;
import static store.engine.StorageService.ANSI_RESET;

record InMemRecord(long offset, String filename, long timestamp) {
}

record WriteRequest(String key, String value, long timestamp, CompletableFuture<String> future) {
}

public class StorageEngine {
    private static final int MAX_BATCH_SIZE = 100;
    private final LogWriter logWriter;
    private ConcurrentMap<String, InMemRecord> keyDir;
    private Map<String, FileChannel> fileToChannelMap;
    private FileChannel readFileChannel;
    private FileChannel writeFileChannel;
    private List<LogWriter> logWriters;
    private BlockingQueue<WriteRequest> writeQueue;
    private final ExecutorService writerThreadPool;

    // TODO: this shouldn't be hardcoded
    private static final String PATH_TO_DATA_FILE = "./data/";
    private static final int WRITE_CHANNELS_COUNT = 1;

    public StorageEngine() throws IOException {
        final String activeFileName = System.currentTimeMillis() + ".data";

        constructFileToChannelMap();
        constructKeyDir();
        constructLogWriters();

        this.writerThreadPool = Executors.newFixedThreadPool(WRITE_CHANNELS_COUNT);
        this.writeQueue = new ArrayBlockingQueue<>(1000);
        startWriterTasks();

        this.logWriter = new LogWriter(writeFileChannel, activeFileName);
    }

    private void constructLogWriters() throws IOException {
        this.logWriters = new ArrayList<>();
        for (int i = 0; i < WRITE_CHANNELS_COUNT; i++) {

            final String activeFileName = System.currentTimeMillis() + ".data";
            writeFileChannel = FileChannel.open(
                    Path.of(PATH_TO_DATA_FILE + activeFileName),
                    StandardOpenOption.CREATE,
                    StandardOpenOption.APPEND);

            final LogWriter logWriter = new LogWriter(writeFileChannel, activeFileName);
            logWriters.add(logWriter);

            readFileChannel = FileChannel.open(
                    Path.of(PATH_TO_DATA_FILE + activeFileName),
                    StandardOpenOption.READ);
            fileToChannelMap.put(activeFileName, readFileChannel);
        }
    }

    public CompletableFuture<String> writeToQueue(final String key, final String value) {
        // System.out.println("Going to write: " + key);
        final CompletableFuture<String> future = new CompletableFuture<>();
        try {
            writeQueue.put(new WriteRequest(key, value, System.currentTimeMillis(), future));
            // System.out.println("Wrote: " + key);
        } catch (InterruptedException e) {
            future.completeExceptionally(e);
        }

        return future;
    }

    private void startWriterTasks() {
        System.out.println("Starting " + logWriters.size() + " writer tasks...");
        for (int i = 0; i < logWriters.size(); i++) {
            writerThreadPool.submit(() -> {
                try {
                    processWriteQueue();
                } catch (InterruptedException e) {
                    System.out.println("Writer task interrupted.");
                    Thread.currentThread().interrupt();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
        }
        // System.out.println("Writer tasks submitted.");
    }

    private void processWriteQueue() throws InterruptedException, IOException {
        while (!Thread.currentThread().isInterrupted()) {
            final List<WriteRequest> batch = new ArrayList<>(MAX_BATCH_SIZE);
            // System.out.println("Waiting to read from queue");

            final WriteRequest writeRequest = writeQueue.take();
            batch.add(writeRequest);
            writeQueue.drainTo(batch, MAX_BATCH_SIZE - 1);

            // System.out.printf("Processing batch of size: %d; write queue size: %d\n", batch.size(), writeQueue.size());

            LogWriter logWriter = getLogWriter(String.valueOf(ThreadLocalRandom.current().nextInt(1, 11)));
            List<Long> offsets = logWriter.writeBatch(batch);

            for (int i = 0; i < batch.size(); i++) {
                WriteRequest request = batch.get(i);
                long offset = offsets.get(i);
                keyDir.put(request.key(), new InMemRecord(offset, logWriter.getActiveFileName(), request.timestamp()));
                request.future().complete(ANSI_GREEN + "OK\n" + ANSI_RESET); // Or some success indicator
            }
        }
    }

    private LogWriter getLogWriter(final String key) {
        final int writerIndex = Math.abs(key.hashCode()) % logWriters.size();
        return logWriters.get(writerIndex);
    }

    public void write(final String key, final String value, final LogWriter logWriter) throws IOException {
        final long timestamp = System.currentTimeMillis();
        // System.out.printf("writing to %s\n", logWriter.getActiveFileName());

        final long offset = logWriter.write(key, value, timestamp); // write to disk
        keyDir.put(key, new InMemRecord(offset, logWriter.getActiveFileName(), timestamp)); // update in memory map

        long MAX_FILE_SIZE = 64 * 1024 * 1024;
        if (writeFileChannel.position() > MAX_FILE_SIZE) {
            updateFileChannels();
        }
    }

    public Boolean exists(final String key) {
        return keyDir.containsKey(key);
    }

    public String read(final String key) throws IOException {
        final InMemRecord inMemRecord =  keyDir.getOrDefault(key, null);

        if (inMemRecord == null) {
            return null;
        }

        System.out.println("Reading from " + inMemRecord.filename());
        final FileChannel readChannel = fileToChannelMap.get(inMemRecord.filename());
        final DiskRecord record = DiskRecord.readFrom(readChannel, inMemRecord.offset());

       if (record != null) {
            return record.value();
        }

        return null;
    }

    private void updateFileChannels() throws IOException {
        final long timestamp = System.currentTimeMillis();
        writeFileChannel.close();
        final String activeFileName = timestamp + ".data";

        writeFileChannel = FileChannel.open(
                Path.of(PATH_TO_DATA_FILE + activeFileName),
                StandardOpenOption.CREATE,
                StandardOpenOption.APPEND);
        logWriter.setFileChannel(writeFileChannel);
        logWriter.setActiveFileName(activeFileName);

        readFileChannel = FileChannel.open(
                Path.of(PATH_TO_DATA_FILE + activeFileName),
                StandardOpenOption.READ);
        fileToChannelMap.put(activeFileName, readFileChannel);
    }

    private void constructFileToChannelMap() throws IOException {
        this.fileToChannelMap = new HashMap<>();
        final File dir = new File(PATH_TO_DATA_FILE);
        final File[] files = dir.listFiles();

        assert files != null;
        for (final File file : files) {
            readFileChannel = FileChannel.open(
                    Path.of(file.getAbsolutePath()),
                    StandardOpenOption.READ);
            fileToChannelMap.put(file.getName(), readFileChannel);
        }

        System.out.printf("File channels loaded for reading: %d\n", fileToChannelMap.size());
    }

    private void constructKeyDir() throws IOException {
        this.keyDir = new ConcurrentHashMap<>();

        // this needs to be ordered, alphabetically
        List<String> sortedFileNames = fileToChannelMap.keySet().stream().sorted(Collections.reverseOrder()).toList();
        long latestFlushTimestamp = Long.MIN_VALUE;

        for (final String filename: sortedFileNames) {
            System.out.printf("Reading data file: %s\n", filename);
            long offset = 0;
            final FileChannel fileChannel = fileToChannelMap.get(filename);

            while (offset < fileChannel.size()) {
                final DiskRecord record = DiskRecord.readFrom(fileChannel, offset);
                if (record != null) {
                    if (record.value().equals(TOMBSTONE_VALUE)) {
                        if (keyDir.containsKey(record.key()) && keyDir.get(record.key()).timestamp() < record.timestamp()) {
                            keyDir.remove(record.key());
                            offset = record.offset() + 2;
                            continue;
                        }
                    }

                    if (record.key().equals(FLUSH_TOMBSTONE_VALUE)) {
                        if (record.timestamp() > latestFlushTimestamp) {
                            latestFlushTimestamp = record.timestamp();
                        }

                        offset = record.offset() + 2;
                        continue;
                    }

                    if (keyDir.containsKey(record.key())) {
                        if (keyDir.get(record.key()).timestamp() > record.timestamp()) {
                            offset = record.offset() + 2;
                            continue;
                        }
                    }

                    keyDir.put(record.key(), new InMemRecord(offset, filename, record.timestamp()));
                    offset = record.offset() + 2;
                } else {
                    break;
                }
            }
        }

        if (latestFlushTimestamp != Long.MIN_VALUE) {
            for (final String key : keyDir.keySet()) {
                // there's no other correct way to do this
                // if we clear keyDir upon seeing
                if (keyDir.get(key).timestamp() < latestFlushTimestamp) {
                    keyDir.remove(key);
                }
            }
        }

        System.out.printf("KeyDir constructed: %d\n", keyDir.size());
    }

    public CompletableFuture<String> delete(final String key) throws IOException {
        keyDir.remove(key);
        System.out.printf("Deleted key: %s\n", key);
        return logWriter.delete(key);
    }

    public CompletableFuture<String> flush() throws IOException, ExecutionException, InterruptedException {
        System.out.printf("KeyDir size before flush: %d\n", keyDir.size());
        keyDir.clear();
        System.out.printf("KeyDir size after flush: %d\n", keyDir.size());
        return logWriter.flush();
    }

    public int getKeyDirSize() {
        return keyDir.size();
    }

    public int getDiskSize() {
        File directory = new File(PATH_TO_DATA_FILE);
        File[] files = directory.listFiles();
        int totalSize = 0;

        if (files != null) {
            for (final File file : files) {
                if (file.isFile()) {
                    totalSize += (int) file.length();
                }
            }
        }
        return totalSize;
    }

    public int getDataFilesCount() {
        File directory = new File(PATH_TO_DATA_FILE);
        File[] files = directory.listFiles();

        if (files != null) {
            return files.length;
        }

        return 0;
    }
}
