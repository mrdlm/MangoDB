package store;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.Map;

record InMemRecord(long offset, String filename, long timestamp) {
}

public class DataFileManager {
    private final LogWriter logWriter;
    private Map<String, InMemRecord> keyDir;
    private Map<String, FileChannel> fileToChannelMap;
    private FileChannel readFileChannel;
    private FileChannel writeFileChannel;

    // TODO: this shouldn't be hardcoded
    private static final String PATH_TO_DATA_FILE = "./data/";

    public DataFileManager() throws IOException {
        final String activeFileName = System.currentTimeMillis() + ".data";

        constructFileToChannelMap();
        constructKeyDir();

        writeFileChannel= FileChannel.open(
                Path.of(PATH_TO_DATA_FILE + activeFileName),
                StandardOpenOption.CREATE,
                StandardOpenOption.APPEND);

        readFileChannel = FileChannel.open(
                Path.of(PATH_TO_DATA_FILE + activeFileName),
                StandardOpenOption.READ);
        fileToChannelMap.put(activeFileName, readFileChannel);

        this.logWriter = new LogWriter(writeFileChannel, activeFileName);
    }

    public void write(final String key, final String value) throws IOException {
        final long timestamp = System.currentTimeMillis();
        System.out.printf("writing to %s\n", logWriter.getActiveFileName());

        final long offset = logWriter.write(key, value, timestamp); // write to disk
        keyDir.put(key, new InMemRecord(offset, logWriter.getActiveFileName(), timestamp)); // update in memory map

        long MAX_FILE_SIZE = 64 * 1024 * 1024;
        if (writeFileChannel.position() > MAX_FILE_SIZE) {
            updateFileChannels();
        }
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
        this.keyDir = new HashMap<>();

        for (final String filename: fileToChannelMap.keySet()) {
            long offset = 0;
            final FileChannel fileChannel = fileToChannelMap.get(filename);

            while (offset < fileChannel.size()) {
                final DiskRecord record = DiskRecord.readFrom(fileChannel, offset);
                if (record != null) {
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

        System.out.printf("KeyDir constructed: %d\n", keyDir.size());
    }
}

