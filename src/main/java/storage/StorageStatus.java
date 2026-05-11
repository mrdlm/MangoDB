package storage;

public record StorageStatus(long diskSizeBytes, int fileCount, int keyCount, long uptimeMillis) {
}
