package store;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

public class MangoDB {
    private final DataFileManager dataFileManager;

    public MangoDB() throws IOException {
        dataFileManager = new DataFileManager();
    }

    public CompletableFuture<String> handle(final String input) throws IOException {
        final List<String> inputList = List.of(input.split(" "));
        // System.out.println(inputList);
        if (inputList.isEmpty()) {
            return CompletableFuture.completedFuture("");
        }

        if (inputList.size() == 3) {
            if (!inputList.get(0).equals("PUT")) {
                return CompletableFuture.completedFuture("Invalid input");
            }

            return dataFileManager.writeToQueue(inputList.get(1), inputList.get(2));
        } else if (inputList.size() == 2) {
            if (!inputList.get(0).equals("GET")) {
                return CompletableFuture.completedFuture("Invalid input");
            }

            final String value = dataFileManager.read(inputList.get(1));
            return CompletableFuture.completedFuture(Objects.requireNonNullElse(value, "Key not found"));
        } else {
            return CompletableFuture.completedFuture("Invalid input");
        }
    }
}
