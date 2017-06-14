package org.corfudb.runtime.view.stream;

import org.corfudb.infrastructure.TestLayoutBuilder;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.view.AbstractViewTest;
import org.corfudb.runtime.view.Layout;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests the BackpointerStreamView
 * <p>
 * Created by zlokhandwala on 5/24/17.
 */
public class BackpointerStreamViewTest extends AbstractViewTest {

    /**
     * Tests the hasNext functionality of the streamView.
     */
    @Test
    public void hasNextTest() {
        CorfuRuntime runtime = getDefaultRuntime();

        IStreamView sv = runtime.getStreamsView().get(CorfuRuntime.getStreamID("streamA"));
        sv.append("hello world".getBytes());

        assertThat(sv.hasNext()).isTrue();
        sv.next();
        assertThat(sv.hasNext()).isFalse();
    }

    /**
     * tests navigating forward/backward on a stream,
     * with intermittent appends to the stream.
     *
     * in addition to correctness assertions, this test can be used for
     * single-stepping with a debugger and observing stream behavior.
     */
    @Test
    public void readQueueTest() {
        CorfuRuntime runtime = getDefaultRuntime();
        IStreamView sv = runtime.getStreamsView().get(CorfuRuntime.getStreamID("streamA"));
        final int ten = 10;

        // initially, populate the stream with appends
        for (int i = 0; i < PARAMETERS.NUM_ITERATIONS_LOW; i++) {
            sv.append(String.valueOf(i).getBytes());
        }

        // travese the stream forward while periodically (every ten
        // iterations) appending to it
        for (int i = 0; i < PARAMETERS.NUM_ITERATIONS_LOW; i++) {
            assertThat(sv.hasNext()).isTrue();
            byte[] payLoad = (byte[]) sv.next().getPayload(runtime);
            assertThat(new String(payLoad).equals(String.valueOf(i)))
                    .isTrue();
            assertThat(sv.getCurrentGlobalPosition()).isEqualTo(i);

            if (i % ten == 1) {
                for (int j = 0; j < PARAMETERS.NUM_ITERATIONS_VERY_LOW; j++) {
                    sv.append(String.valueOf(i).getBytes());
                }

            }
        }

        // traverse the stream backwards, while periodically (every ten
        // iterations) appending to it
        for (int i = PARAMETERS.NUM_ITERATIONS_LOW - 1; i >= 0; i--) {
            byte[] payLoad = (byte[]) sv.current().getPayload(runtime);
            assertThat(new String(payLoad).equals(String.valueOf(i)))
                    .isTrue();
            assertThat(sv.getCurrentGlobalPosition()).isEqualTo(i);
            sv.previous();

            if (i % ten == 1) {
                for (int j = 0; j < PARAMETERS.NUM_ITERATIONS_VERY_LOW; j++) {
                    sv.append(String.valueOf(i).getBytes());
                }

            }
        }
    }


    @Test
    public void moreReadQueueTest() {
        CorfuRuntime runtime = getDefaultRuntime();
        IStreamView sv = runtime.getStreamsView().get(CorfuRuntime.getStreamID("streamA"));
        final int ten = 10;

        // initially, populate the stream with appends
        for (int i = 0; i < PARAMETERS.NUM_ITERATIONS_VERY_LOW; i++) {
            sv.append(String.valueOf(i).getBytes());
        }

        // simple traverse to end of stream
        for (int i = 0; i < PARAMETERS.NUM_ITERATIONS_VERY_LOW; i++) {
            assertThat(sv.hasNext()).isTrue();
            sv.next();
        }

        // add two entries on alternate steps, and traverse forward one at a
        // time
        for (int i = 0; i < PARAMETERS.NUM_ITERATIONS_VERY_LOW; i++) {
            if (i % 2 == 0) {
                assertThat(sv.hasNext()).isFalse();
                sv.append(String.valueOf(i).getBytes());
                sv.append(String.valueOf(i).getBytes());
            }
            byte[] payLoad = (byte[]) sv.next().getPayload(runtime);
            assertThat(new String(payLoad).equals(String.valueOf(i)));
        }
    }



}
