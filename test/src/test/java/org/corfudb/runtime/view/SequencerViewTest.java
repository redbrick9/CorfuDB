package org.corfudb.runtime.view;

import lombok.Getter;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.protocols.wireprotocol.TokenRequest;
import org.corfudb.runtime.CorfuRuntime;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by mwei on 12/23/15.
 */
public class SequencerViewTest extends AbstractViewTest {

    @Getter
    final String defaultConfigurationString = getDefaultEndpoint();

    @Test
    public void canAcquireFirstToken() {
        CorfuRuntime r = getDefaultRuntime();
        assertThat(r.getSequencerView().nextToken(Collections.emptyList(), 1).getToken())
                .isEqualTo(new Token(0L, 0L));
    }

    @Test
    public void canQueryMultipleStreams() {
        CorfuRuntime r = getDefaultRuntime();

        UUID stream1 = UUID.randomUUID();
        UUID stream2 = UUID.randomUUID();
        UUID stream3 = UUID.randomUUID();

        assertThat(r.getSequencerView().nextToken(Collections.singletonList(stream1), 1).getToken())
                .isEqualTo(new Token(0l, 0l));
        assertThat(r.getSequencerView().nextToken(Collections.singletonList(stream2), 1).getToken())
                .isEqualTo(new Token(1l, 0l));

        List<UUID> tailsToQuery = Arrays.asList(stream1, stream2, stream3);

        assertThat(r.getSequencerView().nextToken(tailsToQuery, 0).getStreamTails())
                .containsExactly(0l, 1l, Address.NON_EXIST);
    }

    @Test
    public void tokensAreIncrementing() {
        CorfuRuntime r = getDefaultRuntime();
        assertThat(r.getSequencerView().nextToken(Collections.emptyList(), 1).getToken())
                .isEqualTo(new Token(0L, 0L));
        assertThat(r.getSequencerView().nextToken(Collections.emptyList(), 1).getToken())
                .isEqualTo(new Token(1L, 0L));
    }

    @Test
    public void checkTokenWorks() {
        CorfuRuntime r = getDefaultRuntime();
        assertThat(r.getSequencerView().nextToken(Collections.emptyList(), 1).getToken())
                .isEqualTo(new Token(0L, 0L));
        assertThat(r.getSequencerView().nextToken(Collections.emptyList(), 0).getToken())
                .isEqualTo(new Token(0L, 0L));
    }

    @Test
    public void checkStreamTokensWork() {
        CorfuRuntime r = getDefaultRuntime();
        UUID streamA = UUID.nameUUIDFromBytes("stream A".getBytes());
        UUID streamB = UUID.nameUUIDFromBytes("stream B".getBytes());

        assertThat(r.getSequencerView().nextToken(Collections.singletonList(streamA), 1).getToken())
                .isEqualTo(new Token(0L, 0L));
        assertThat(r.getSequencerView().nextToken(Collections.singletonList(streamA), 0).getToken())
                .isEqualTo(new Token(0L, 0L));
        assertThat(r.getSequencerView().nextToken(Collections.singletonList(streamB), 1).getToken())
                .isEqualTo(new Token(1L, 0L));
        assertThat(r.getSequencerView().nextToken(Collections.singletonList(streamB), 0).getToken())
                .isEqualTo(new Token(1L, 0L));
        assertThat(r.getSequencerView().nextToken(Collections.singletonList(streamA), 0).getToken())
                .isEqualTo(new Token(0L, 0L));
    }

    @Test
    public void checkBackPointersWork() {
        CorfuRuntime r = getDefaultRuntime();
        UUID streamA = UUID.nameUUIDFromBytes("stream A".getBytes());
        UUID streamB = UUID.nameUUIDFromBytes("stream B".getBytes());

        assertThat(r.getSequencerView().nextToken(Collections.singletonList(streamA), 1).getBackpointerMap())
                .containsEntry(streamA, Address.NON_EXIST);
        assertThat(r.getSequencerView().nextToken(Collections.singletonList(streamA), 0).getBackpointerMap())
                .isEmpty();
        assertThat(r.getSequencerView().nextToken(Collections.singletonList(streamB), 1).getBackpointerMap())
                .containsEntry(streamB, Address.NON_EXIST);
        assertThat(r.getSequencerView().nextToken(Collections.singletonList(streamB), 0).getBackpointerMap())
                .isEmpty();
        assertThat(r.getSequencerView().nextToken(Collections.singletonList(streamA), 1).getBackpointerMap())
                .containsEntry(streamA, 0L);
        assertThat(r.getSequencerView().nextToken(Collections.singletonList(streamB), 1).getBackpointerMap())
                .containsEntry(streamB, 1L);
    }
}
