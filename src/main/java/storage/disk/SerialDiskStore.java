package storage.disk;

import storage.AsyncWriteRequest;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class SerialDiskStore implements DiskStore {

    @Override
    public String read(long offset, final FileChannel readFileChannel) {
        try {
            final DiskRecord record = DiskRecord.readFrom(readFileChannel, offset);
            return record.value();
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<Long> write(List<AsyncWriteRequest> batch, FileChannel fileChannel) {
        if (batch == null || batch.isEmpty()) {
            return new ArrayList<>();
        }

        int totalSize = 0;
        List<byte[]> keyBytesList = new ArrayList<>(batch.size());
        List<byte[]> valueBytesList = new ArrayList<>(batch.size());

        // we go through the list once to calculate the total size of the batch
        for (AsyncWriteRequest request: batch) {
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
                AsyncWriteRequest request = batch.get(i);
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
            throw new RuntimeException(e);
        }
    }

    @Override
    public void delete(final long offset, final FileChannel writeFileChannel) {
    }

    @Override
    public void flush() {
    }
}
