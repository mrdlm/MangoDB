package storage.mem; import config.ConfigManager;
import org.slf4j.Logger; import org.slf4j.LoggerFactory; import storage.disk.DiskRecord;

import java.io.File;
import java.io.IOException; import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Collections; import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import static legacy.engine.LogWriter.FLUSH_TOMBSTONE_VALUE;
import static legacy.engine.LogWriter.TOMBSTONE_VALUE;

public class UnsafeMemStore implements MemStore {
    // not thread safe, but fast
    private static Logger logger = LoggerFactory.getLogger(UnsafeMemStore.class);
    private String DATA_PATH;
    final ConcurrentHashMap<String, MemRecord> keyDir = new ConcurrentHashMap<>();

    public UnsafeMemStore() {
        DATA_PATH = new ConfigManager("config.properties").getProperty("datapath");
        try {
            loadKeyDir();
        } catch (IOException e) {
            logger.error("Unable to load KeyDir", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void write(String key, long valueOffset, String filename, long timestamp) {
       MemRecord memRecord = new MemRecord(valueOffset, filename, timestamp);
       keyDir.put(key, memRecord);
    }

    @Override
    public Optional<MemRecord> read(String key) {
        if (!keyDir.containsKey(key)) {
            return Optional.empty();
        }

        return Optional.of(keyDir.get(key));
    }

    private void loadKeyDir() throws IOException {
        logger.info("Loading data to memstore");
        final Map<String, FileChannel> fileChannels = new HashMap<>();

        final File dir = new File(DATA_PATH);
        final File[] files = dir.listFiles();

        assert files != null;
        for (final File file : files) {
            FileChannel readFileChannel = FileChannel.open(
                    Path.of(file.getAbsolutePath()),
                    StandardOpenOption.READ);
            fileChannels.put(file.getName(), readFileChannel);
        }

        List<String> sortedFileNames = fileChannels.keySet().stream().sorted(Collections.reverseOrder()).toList();
        long latestFlushTimestamp = Long.MIN_VALUE;

        for (final String filename: sortedFileNames) {
            logger.info("Reading data file: {}", filename);
            long offset = 0;
            final FileChannel fileChannel = fileChannels.get(filename);

            while (offset < fileChannel.size()) {
                logger.info("offset {}, filechannelsize {}", offset, fileChannel.size());

                // this should ideally be reading from DiskStore.
                // disk data shouldn't directly be exposed to UnsafeMemStore
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

                    keyDir.put(record.key(), new MemRecord(offset, filename, record.timestamp()));
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

        logger.info("KeyDir constructed finished, size: {}\n", keyDir.size());
    }
}
