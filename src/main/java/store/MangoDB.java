package store;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public class MangoDB {
    private final DataFileManager dataFileManager;

    public MangoDB() throws IOException {
        dataFileManager = new DataFileManager();
    }

    public String handle(final String input) throws IOException {
        final List<String> inputList = List.of(input.split(" "));

        if (inputList.isEmpty()) {
            return "";
        }

        if (inputList.size() == 3) {
            if (!inputList.get(0).equals("PUT")) {
                return "Invalid input";
            }

            dataFileManager.write(inputList.get(1), inputList.get(2));
        } else if (inputList.size() == 2) {
            if (!inputList.get(0).equals("GET")) {
                return "Invalid input";
            }

            final String value = dataFileManager.read(inputList.get(1));
            return Objects.requireNonNullElse(value, "Key not found");
        } else {
            return "Invalid input";
        }

        return "success";
    }
}
