import store.StorageEngine;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.Scanner;

public class Main {
    public static void main(String[] args) throws IOException {

        final StorageEngine storageEngine = new StorageEngine();
        final Scanner scanner = new Scanner(System.in);

        while (true) {
            System.out.print("mango> ");
            final String input = scanner.nextLine();

            final List<String> inputList = List.of(input.split(" "));

            if (inputList.isEmpty()) {
                continue;
            }

            if (inputList.size() == 3) {
                if (!inputList.get(0).equals("PUT")) {
                    System.out.println("Invalid input");
                    continue;
                }
                storageEngine.writeToQueue(inputList.get(1), inputList.get(2));
            } else if (inputList.size() == 2) {
                if (!inputList.get(0).equals("GET")) {
                    System.out.println("Invalid input");
                    continue;
                }

                final String value = storageEngine.read(inputList.get(1));
                System.out.println(Objects.requireNonNullElse(value, "Key not found"));
            } else {
                System.out.println("Invalid input");
            }
        }
    }
}