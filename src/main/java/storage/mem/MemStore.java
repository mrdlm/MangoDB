package storage.mem;

import java.util.Optional;

public interface MemStore {
   void write(String key, long valueOffset, String filename, long timestamp);
   Optional<MemRecord> read(String key);
}
