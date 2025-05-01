package storage.disk;


import storage.AsyncWriteRequest;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public interface DiskStore {
    String read(long offset, FileChannel readFileChannel) throws IOException;
    List<WriteResult> write(List<AsyncWriteRequest> batch, FileChannel writeFileChannel);
    void delete(long offset, FileChannel writeFileChannel);
    void flush();
}

