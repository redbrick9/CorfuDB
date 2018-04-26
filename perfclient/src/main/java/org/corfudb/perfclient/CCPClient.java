package org.corfudb.perfclient;

import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.SMRMap;
import org.corfudb.runtime.view.stream.IStreamView;
import org.corfudb.util.serializer.Serializers;

import java.util.UUID;

public class CCPClient {

    public static void main(String[] args) throws Exception  {

        String connectionString = args[0];
        int numClients = Integer.valueOf(args[1]);
        int numThreads = Integer.valueOf(args[2]);
        int opsPerThread = Integer.valueOf(args[3]);
        int sizeOfPayload = Integer.valueOf(args[4]);

        CorfuRuntime[] runtimes = new CorfuRuntime[numClients];
        SMRMap<UUID, byte[]>[] maps = new SMRMap[numClients];
        IStreamView[] streams = new IStreamView[numClients];
        String mapName = "map1";
        String streamName = "stream1";

        for (int x = 0; x < numClients; x++) {
            runtimes[x] = new CorfuRuntime(connectionString).connect();
            maps[x] = runtimes[x].getObjectsView()
                    .build()
                    .setStreamName(mapName)
                    .setSerializer(Serializers.JAVA)
                    .setType(SMRMap.class)
                    .open();
            streams[x] = runtimes[x].getStreamsView().get(CorfuRuntime.getStreamID(streamName));
        }

        boolean streamWrites = false;

        Thread[] writerThreads = new Thread[numThreads];
        if (!streamWrites) {
            for (int x = 0; x < numThreads; x++) {
                Runnable r = () -> {
                    byte[] payload = new byte[sizeOfPayload];

                    for (int y = 0; y < opsPerThread; y++) {
                        maps[y % numClients].blindPut(UUID.randomUUID(), payload);
                    }
                };

                writerThreads[x] = new Thread(r);
            }

        } else {

            for (int x = 0; x < numThreads; x++) {
                Runnable r = () -> {
                    byte[] payload = new byte[sizeOfPayload];

                    for (int y = 0; y < opsPerThread; y++) {
                        streams[y % numClients].append(payload);
                    }
                };

                writerThreads[x] = new Thread(r);
            }
        }


        long s1 = System.currentTimeMillis();

        for (int x = 0; x < numThreads; x++) {
            writerThreads[x].start();
        }

        for (int x = 0; x < numThreads; x++) {
            writerThreads[x].join();
        }

        long s2 = System.currentTimeMillis();

        System.out.println("totalTime(ms) " + (s2 - s1));
        System.out.println("totalOps " + (opsPerThread * numThreads));
        System.out.println("throughput " + (((opsPerThread * numThreads) * 1.0) / (s2 - s1)));
    }

}
