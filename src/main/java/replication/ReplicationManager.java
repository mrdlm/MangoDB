package replication;

import client.MangoClient;
import client.MangoTreeClient;
import client.ServerAddress;
import storage.AsyncWriteRequest;
import storage.disk.WriteRequest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class ReplicationManager {

    private final List<MangoClient> secondaryMangoClients;
    private final MangoTreeClient treeClient;
    private final Thread replicationThread;
    private final BlockingQueue<WriteRequest> replicationQueue;
    private final Set<String> secondaryIds;

    public ReplicationManager() {
        // this shouldn't be hardcoded
        this.treeClient = new MangoTreeClient("localhost", 9090);
        treeClient.connect();

        System.out.println("ReplicationManager successfully connected to MangoTree");
        this.secondaryIds = new HashSet<>();
        this.secondaryMangoClients = new ArrayList<>();
        refreshSecondaryClients();

        this.replicationQueue = new ArrayBlockingQueue<>(1000);

        this.replicationThread = new Thread(this::replicateToSecondaries);
        this.replicationThread.start();
    }

    public void asyncReplicate(final String key, final String value) {
        final WriteRequest writeRequest = new WriteRequest(key, value, System.currentTimeMillis());
        replicationQueue.add(writeRequest);
    }

    public void replicateToSecondaries() {
        while (!Thread.currentThread().isInterrupted()) {
           if (!replicationQueue.isEmpty()) {
               final WriteRequest request = replicationQueue.poll();

               try {
                   if (secondaryMangoClients.isEmpty()) {
                       System.out.println("No secondary clients available for replication; refreshing...");
                       refreshSecondaryClients();
                   }

                   for (final MangoClient secondaryClient : secondaryMangoClients) {
                       secondaryClient.put(request.key(), request.value());
                   }
               } catch (final Exception e) {
                   System.err.println("Failed to replicate to secondary: " + e.getMessage());
                   // refresh secondary clients and retry
                   refreshSecondaryClients();
                   replicationQueue.add(request); // push back to queue
               }
           }
        }
    }

    private void refreshSecondaryClients() {
        try {
            final List<ServerAddress> latestSecondaries = treeClient.getSecondaryAddresses();
            List<MangoClient> newSecondaryClients = new ArrayList<>();
            Set<String> potentiallyDeadSecondaries = new HashSet<>(secondaryIds);

            for (final ServerAddress latestSecondary : latestSecondaries) {
                final String id = String.format("%s:%d", latestSecondary.host(), latestSecondary.port());

                if (!secondaryIds.contains(id)) {
                    final MangoClient secondaryClient = new MangoClient(latestSecondary.host(), latestSecondary.port());
                    secondaryClient.connect();
                    newSecondaryClients.add(secondaryClient);
                    secondaryIds.add(id);
                } else {
                    // if it's still there, it's not dead
                    potentiallyDeadSecondaries.remove(id);
                }
            }

            for (final MangoClient secondaryClient : secondaryMangoClients) {
                if (potentiallyDeadSecondaries.contains(secondaryClient.getId())) {
                    secondaryMangoClients.remove(secondaryClient);
                    secondaryIds.remove(secondaryClient.getId());
                }
            }

            secondaryMangoClients.addAll(newSecondaryClients);
        } catch (IOException e) {
            System.err.println("Failed to refresh secondary clients: " + e.getMessage());
            throw new RuntimeException(e);
        }
    }
}
