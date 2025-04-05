package org.example.store;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;

public class LogWriter {
    private FileChannel fileChannel;

    public LogWriter(FileChannel fileChannel) {
        this.fileChannel = fileChannel;
    }

    public void setFileChannel(final FileChannel fileChannel) {
       this.fileChannel = fileChannel;
    }

    public long write(final String key, final String value) throws IOException {
        try {
            final byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
            final byte[] valueBytes = value.getBytes(StandardCharsets.UTF_8);
            final long timestamp = System.currentTimeMillis();

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

            System.out.printf("Writing to disk (key: %s, value: %s) \n", key, value);
            return startPosition;
        } catch (final Exception e) {
            System.out.println("Error writing log: " + e.getMessage());
            throw new RuntimeException(e);
        }
    }

    public void flush() throws IOException {
        fileChannel.force(true);
    }
}
