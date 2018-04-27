package org.corfudb.integration;

import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.SMRMap;
import org.corfudb.runtime.exceptions.WriteSizeException;
import org.junit.Test;

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
        rt.getParameters().setEnableMultiStreamQuery(true);

        Map<String, String> map = rt.getObjectsView().build().setStreamName("s1").setType(SMRMap.class).open();
        Map<String, String> map2 = rt.getObjectsView().build().setStreamName("s2").setType(SMRMap.class).open();

        int numThread = 10;
        Thread[] threads = new Thread[numThread];
        System.out.println("start");
        for (int x = 0; x < numThread; x++) {
            Runnable r = () -> {

                for (int y = 0; y < 50; y++) {
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
    }
}
