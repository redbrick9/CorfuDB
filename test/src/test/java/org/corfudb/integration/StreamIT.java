package org.corfudb.integration;

import org.corfudb.runtime.BootstrapUtil;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.WriteSizeException;
import org.corfudb.runtime.view.Layout;
import org.junit.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A set integration tests that exercise the stream API.
 */

public class StreamIT {

    @Test
    public void largeStreamWrite() throws Exception {

        Layout layout = new Layout(
                new ArrayList<>(
                        Arrays.asList("localhost:9000", "localhost:9001")),
                new ArrayList<>(
                        Arrays.asList("localhost:9000", "localhost:9001")),
                Collections.singletonList(new Layout.LayoutSegment(
                        Layout.ReplicationMode.CHAIN_REPLICATION,
                        0L,
                        -1L,
                        Collections.singletonList(new Layout.LayoutStripe(
                                Arrays.asList("localhost:9000", "localhost:9001")
                        )))),
                0L,
                UUID.randomUUID());

        BootstrapUtil.bootstrap(layout, 10, Duration.ofSeconds(1));

    }
}
