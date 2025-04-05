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
    private final Map<String, FileChannel> keyFileMap;
    private FileChannel readFileChannel;
    private FileChannel writeFileChannel;
    private static long MAX_FILE_SIZE = 50;

    // TODO: this shouldn't be hardcoded
    private static final String PATH_TO_DATA_FILE = "/Users/mmohan/Personal/projects/MangoDB/data/";

    public DataFileManager() throws IOException {
        final String activeFileName = System.currentTimeMillis() + ".data";

        keyFileMap = new HashMap<>();

        writeFileChannel= FileChannel.open(
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
        keyFileMap.put(key, readFileChannel);
        logWriter.flush();

        System.out.println(writeFileChannel.position());
        if (writeFileChannel.position() > MAX_FILE_SIZE) {
            writeFileChannel.close();
            final String activeFileName = System.currentTimeMillis() + ".data";

            writeFileChannel = FileChannel.open(
                    Path.of(PATH_TO_DATA_FILE + activeFileName),
                    StandardOpenOption.CREATE,
                    StandardOpenOption.APPEND);
            logWriter.setFileChannel(writeFileChannel);

            readFileChannel.close();
            readFileChannel = FileChannel.open(
                    Path.of(PATH_TO_DATA_FILE + activeFileName),
                    StandardOpenOption.READ);
        }
    }

    public String read(final String key) throws IOException {
        final FileChannel readChannel = keyFileMap.get(key);
        final Long offset = keyDir.getOrDefault(key, null);

        if (offset != null) {
            LogRecord record = LogRecord.readFrom(readChannel, offset);
            return record.getValue();
        }

        return null;
    }
}

