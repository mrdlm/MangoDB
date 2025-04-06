package store;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class LogRecord {
    private final long timestamp;
    private final String key;
    private final String value;

    public LogRecord(final long timestamp, final String key, final String value) {
        this.timestamp = timestamp;
        this.key = key;
        this.value = value;
    }

    public static LogRecord readFrom(final FileChannel channel, final long offset) throws IOException {
        channel.position(offset);
        // reading the fixed header
        final ByteBuffer header = ByteBuffer.allocate(Long.BYTES + 2 * Integer.BYTES);
        channel.read(header);

        // switch to read mode
        header.flip();

        long timestamp = header.getLong();
        int keyLength = header.getInt();
        int valueLength = header.getInt();

        final ByteBuffer data = ByteBuffer.allocate(keyLength + valueLength);
        channel.read(data);
        data.flip();

        byte[] keyBytes = new byte[keyLength];
        byte[] valueBytes = new byte[valueLength];

        data.get(keyBytes);
        data.get(valueBytes);

        return new LogRecord(timestamp, new String(keyBytes), new String(valueBytes));
    }

    public String getValue() {
        return this.value;
    }
}
