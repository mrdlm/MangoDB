package storage.disk;

public record WriteRequest(String key, String value, long timestamp) { }
