package client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class MangoTreeClient extends MangoClient {
    public MangoTreeClient(final String host, final int port) {
        super(host, port);
    }

    public List<ServerAddress> getSecondaryAddresses() throws IOException {
        final List<ServerAddress> result = new ArrayList<>();
        try {
            if (!connected) {
                connect();
            }
        } catch (final RuntimeException e) {
            System.out.println("Unable to ");
        }

        out.println("SECONDARIES");
        out.flush();
        String response = in.readLine();

        System.out.println("Response: " + response);

        if (Objects.equals(response, "")) {
            return result;
        }

        if (response.contains("ERROR")) {
            throw new IOException(response);
        }

        // a single line containing list of hosts and ports
        List<String> splits = List.of(response.split(" "));

        if (splits.size() % 2 != 0) {
            throw new IOException("Invalid response from server: " + response);
        }

        for (int i = 0 ; i < splits.size() - 1; i += 2) {
           result.add(new ServerAddress(splits.get(i), Integer.parseInt(splits.get(i + 1))));
        }

        return result;
    }
}

