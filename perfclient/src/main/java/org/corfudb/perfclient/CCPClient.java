package org.corfudb.perfclient;

import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.SMRMap;
import org.corfudb.util.serializer.Serializers;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class CCPClient {

    public static void main(String[] args) throws Exception  {

        String connectionString = args[0];
        int numClients = Integer.valueOf(args[1]);
        int numThreads = Integer.valueOf(args[2]);
        int opsPerThread = Integer.valueOf(args[3]);
        int sizeOfPayload = Integer.valueOf(args[4]);
        int batchSize = Integer.valueOf(args[5]);

        CorfuRuntime[] runtimes = new CorfuRuntime[numClients];
        SMRMap<UUID, byte[]>[] maps = new SMRMap[numClients];
        String mapName = "map1";
        for (int x = 0; x < numClients; x++) {
            runtimes[x] = new CorfuRuntime(connectionString).connect();
            maps[x] = runtimes[x].getObjectsView()
                    .build()
                    .setStreamName(mapName)
                    .setSerializer(Serializers.JAVA)
                    .setType(SMRMap.class)
                    .open();
        }

        Thread[] writerThreads = new Thread[numThreads];

        for (int x = 0; x < numThreads; x++) {
            final int clientId = x;

            Runnable r = () -> {

                byte[] payload = new byte[sizeOfPayload];

                for (int a = 0; a < opsPerThread; ) {
                    Map<UUID, byte[]> batch = new HashMap<>(batchSize);
                    for (int b = 0; b < batchSize; b++) {
                        batch.put(UUID.randomUUID(), payload);
                        a++;
                    }
                    maps[clientId].putAll(batch);
                }
            };

            writerThreads[x] = new Thread(r);
        }


        long s1 = System.currentTimeMillis();

        for (int x = 0; x < numThreads; x++) {
            writerThreads[x].start();
        }

        for (int x = 0; x < numThreads; x++) {
            writerThreads[x].join();
        }

        long s2 = System.currentTimeMillis();

        System.out.println("sizeOfMap " + maps[0].size());
        System.out.println("totalTime(ms) " + (s2 - s1));
        System.out.println("totalOps " + (opsPerThread * numThreads));
        System.out.println("throughput " + (((opsPerThread * numThreads) * 1.0) / (s2 - s1)));
    }

}
