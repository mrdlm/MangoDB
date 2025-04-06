package store;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public record DiskRecord(long timestamp, String key, String value, long offset) {

    public static DiskRecord readFrom(final FileChannel channel, final long offset) throws IOException {
        if (offset > channel.size()) {
            return null;
        }

        channel.position(offset);
        // reading the fixed header
        final ByteBuffer header = ByteBuffer.allocate(Long.BYTES + 2 * Integer.BYTES);
        channel.read(header);

        // switch to read mode
        header.flip();

        long timestamp = header.getLong();
        int keyLength = header.getInt();
        int valueLength = header.getInt();

        int recordSize = header.capacity() + keyLength + valueLength;
        final ByteBuffer data = ByteBuffer.allocate(keyLength + valueLength);
        channel.read(data);
        data.flip();

        byte[] keyBytes = new byte[keyLength];
        byte[] valueBytes = new byte[valueLength];

        data.get(keyBytes);
        data.get(valueBytes);

        return new DiskRecord(timestamp, new String(keyBytes), new String(valueBytes), offset + recordSize);
    }
}
