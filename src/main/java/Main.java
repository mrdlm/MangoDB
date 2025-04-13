import store.DataFileManager;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.Scanner;

public class Main {
    public static void main(String[] args) throws IOException {

        final DataFileManager dataFileManager = new DataFileManager();
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
                dataFileManager.writeToQueue(inputList.get(1), inputList.get(2));
            } else if (inputList.size() == 2) {
                if (!inputList.get(0).equals("GET")) {
                    System.out.println("Invalid input");
                    continue;
                }

                final String value = dataFileManager.read(inputList.get(1));
                System.out.println(Objects.requireNonNullElse(value, "Key not found"));
            } else {
                System.out.println("Invalid input");
            }
        }
    }
}