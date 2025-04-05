package org.example.store;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.Map;

public class DataFileManager {
    private final LogWriter logWriter;
    private final Map<String, Long> keyDir;
    private final FileChannel readFileChannel;

    // TODO: this shouldn't be hardcoded
    private static final String PATH_TO_DATA_FILE = "/Users/mmohan/Personal/projects/MangoDB/data/";

    public DataFileManager() throws IOException {
        final String activeFileName = System.currentTimeMillis() + ".data";

        FileChannel writeFileChannel= FileChannel.open(
                Path.of(PATH_TO_DATA_FILE + activeFileName),
                StandardOpenOption.CREATE,
                StandardOpenOption.APPEND);

        readFileChannel = FileChannel.open(
                Path.of(PATH_TO_DATA_FILE + activeFileName),
                StandardOpenOption.READ);

        this.logWriter = new LogWriter(writeFileChannel);
        this.keyDir = new HashMap<>();
    }

    public void write(final String key, final String value) throws IOException {
        final long offset = logWriter.write(key, value);

        keyDir.put(key, offset);
        logWriter.flush();
    }

    public String read(final String key) throws IOException {
        final Long offset = keyDir.getOrDefault(key, null);

        if (offset != null) {
            LogRecord record = LogRecord.readFrom(readFileChannel, offset);
            return record.getValue();
        }

        return null;
    }
}

