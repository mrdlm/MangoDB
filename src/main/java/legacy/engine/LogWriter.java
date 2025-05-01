package legacy.engine;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static legacy.engine.StorageService.ANSI_GREEN;
import static legacy.engine.StorageService.ANSI_RESET;

public class LogWriter {
    public static final String TOMBSTONE_VALUE = "__TOMBSTONE__";
    public static final String FLUSH_TOMBSTONE_VALUE = "__FLUSH_TOMBSTONE__";
    private FileChannel fileChannel;
    private String activeFileName;

    public LogWriter(final FileChannel fileChannel, final String activeFileName) {
        this.fileChannel = fileChannel;
        this.activeFileName = activeFileName;
    }

    public void setActiveFileName(final String activeFileName) {
        this.activeFileName = activeFileName;
    }

    public void setFileChannel(final FileChannel fileChannel) {
       this.fileChannel = fileChannel;
    }

    public String getActiveFileName() {
        return this.activeFileName;
    }

    public long write(final String key, final String value, final long timestamp) throws IOException {
        try {
            final byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
            final byte[] valueBytes = value.getBytes(StandardCharsets.UTF_8);

            // total size = 8 (timestamp) + 4 (keyLen) + 4 (valueLen) + key + value
            final ByteBuffer buffer = ByteBuffer.allocate(
                    Long.BYTES + Integer.BYTES + Integer.BYTES + keyBytes.length + valueBytes.length + 2
            );

            buffer.putLong(timestamp);
            buffer.putInt(keyBytes.length);
            buffer.putInt(valueBytes.length);
            buffer.put(keyBytes);
            buffer.put(valueBytes);
            buffer.putChar('\n');
            buffer.flip();

            final long startPosition = fileChannel.position();
            fileChannel.write(buffer);

            // System.out.printf("Writing to disk (key: %s, value: %s) \n", key, value);
            return startPosition;
        } catch (final Exception e) {
            System.out.println("Error writing log: " + e.getMessage());
            throw new RuntimeException(e);
        }
    }


    public List<Long> writeBatch(final List<WriteRequest> batch) throws IOException {
        if (batch == null || batch.isEmpty()) {
            return new ArrayList<>();
        }

        int totalSize = 0;
        List<byte[]> keyBytesList = new ArrayList<>(batch.size());
        List<byte[]> valueBytesList = new ArrayList<>(batch.size());

        for (WriteRequest request : batch) {
            byte[] keyBytes = request.key().getBytes(StandardCharsets.UTF_8);
            byte[] valueBytes = request.value().getBytes(StandardCharsets.UTF_8);
            keyBytesList.add(keyBytes);
            valueBytesList.add(valueBytes);
            totalSize += Long.BYTES + Integer.BYTES + Integer.BYTES + keyBytes.length + valueBytes.length + 2;
        }

        final ByteBuffer batchBuffer = ByteBuffer.allocate(totalSize);
        final List<Long> offsets = new ArrayList<>(batch.size());
        long currentOffset = -1; // Will be set before the first write

        try {
            final long batchStartOffset = fileChannel.position();
            currentOffset = batchStartOffset;

            for (int i = 0; i < batch.size(); i++) {
                WriteRequest request = batch.get(i);
                byte[] keyBytes = keyBytesList.get(i);
                byte[] valueBytes = valueBytesList.get(i);

                offsets.add(currentOffset); // Store offset for this record

                batchBuffer.putLong(request.timestamp()); // Use request timestamp
                batchBuffer.putInt(keyBytes.length);
                batchBuffer.putInt(valueBytes.length);
                batchBuffer.put(keyBytes);
                batchBuffer.put(valueBytes);
                batchBuffer.putChar('\n'); // Use the same record separator

                currentOffset += Long.BYTES + Integer.BYTES + Integer.BYTES + keyBytes.length + valueBytes.length + 2;
            }

            batchBuffer.flip();
            while(batchBuffer.hasRemaining()) {
                fileChannel.write(batchBuffer);
            }

            // System.out.printf("Wrote batch of %d records to disk (%s)\n", batch.size(), activeFileName);
            return offsets;
        } catch (final IOException e) {
            System.err.println("Error writing batch log: " + e.getMessage());
            // Let the caller handle completing futures exceptionally
            throw e;
        }
    }

    public CompletableFuture<String> delete(final String key) throws IOException {
        final long timestamp = System.currentTimeMillis();
        final CompletableFuture<String> future = new CompletableFuture<>();

        final List<WriteRequest> batch = List.of(new WriteRequest(key, TOMBSTONE_VALUE, timestamp, future));
        writeBatch(batch);

        future.complete(ANSI_GREEN+ "OK\n" + ANSI_RESET);
        return future;
    }

    public CompletableFuture<String> flush() throws IOException {
        final long timestamp = System.currentTimeMillis();
        final CompletableFuture<String> future = new CompletableFuture<>();

        final List<WriteRequest> batch = List.of(new WriteRequest(FLUSH_TOMBSTONE_VALUE, "", timestamp, future));
        writeBatch(batch);

        future.complete(ANSI_GREEN+ "OK\n" + ANSI_RESET);
        return future;
    }
}
