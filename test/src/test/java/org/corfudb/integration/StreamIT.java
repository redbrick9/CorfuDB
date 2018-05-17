package org.corfudb.integration;

import org.corfudb.protocols.wireprotocol.TokenResponse;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.SMRMap;
import org.corfudb.runtime.exceptions.WriteSizeException;
import org.junit.Test;

import java.util.Collections;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A set integration tests that exercise the stream API.
 */

public class StreamIT  {

    @Test
    public void largeStreamWrite() throws Exception {

        CorfuRuntime rt = new CorfuRuntime("localhost:9000").connect();
        rt.getParameters().setEnableMultiStreamQuery(false);

        Map<String, String> map = rt.getObjectsView().build().setStreamName("s1").setType(SMRMap.class).open();
        Map<String, String> map2 = rt.getObjectsView().build().setStreamName("s2").setType(SMRMap.class).open();

        int numThread = 30;
        final int numOps = 100;
        Thread[] threads = new Thread[numThread];
        long s1 = System.currentTimeMillis();
        for (int x = 0; x < numThread; x++) {
            Runnable r = () -> {

                for (int y = 0; y < numOps; y++) {
                    rt.getObjectsView().TXBegin();

                    map.get("a");
                    map.put(Thread.currentThread().getName() + y, String.valueOf(y));
                    map2.get("a2");
                    map2.put(Thread.currentThread().getName() + y, "s3");
                    rt.getObjectsView().TXEnd();
                    map2.get("a2");
                    rt.getStreamsView().get(CorfuRuntime.getStreamID("ss3")).append(new byte[100]);
                }

            };


            threads[x] = new Thread(r);
            threads[x].start();
        }


        for (int x = 0; x < numThread; x++) {
            threads[x].join();
        }

        long s2 = System.currentTimeMillis();

        System.out.println((numThread * numOps * 1.0) / (s2 - s1));
    }

    @Test
    public void largeStreamWrite2() throws Exception {

        CorfuRuntime rt = new CorfuRuntime("localhost:9000").connect();
        rt.getParameters().setEnableMultiStreamQuery(false);



        int numThread = 60;
        final int numOps = 40000;
        final int numStreams = 400;

        for (int x = 0; x < numStreams; x++) {
            rt.getSequencerView().nextToken(Collections.singleton(CorfuRuntime.getStreamID(String.valueOf(x))), 1);
        }

        TokenResponse res = rt.getSequencerView().nextToken(Collections.EMPTY_SET, Integer.MAX_VALUE);
        System.out.println("Number of streams " + res.getStreamsMap().size());

        Thread[] threads = new Thread[numThread];
        long s1 = System.currentTimeMillis();
        for (int x = 0; x < numThread; x++) {
            Runnable r = () -> {
                for (int y = 0; y < numOps; y++) {
                    TokenResponse resLocal = rt.getSequencerView().nextToken(Collections.EMPTY_SET,
                            0);
                    res.getStreamsMap();
                }

            };

            threads[x] = new Thread(r);
            threads[x].start();
        }


        for (int x = 0; x < numThread; x++) {
            threads[x].join();
        }

        long s2 = System.currentTimeMillis();

        System.out.println("total time " + (s2 - s1));
        System.out.println((numThread * numOps * 1.0) / (s2 - s1));
    }
}
