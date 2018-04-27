package org.corfudb.protocols.wireprotocol;

import io.netty.buffer.ByteBuf;
import lombok.Data;

import java.util.Collections;
import java.util.Map;
import java.util.UUID;

/**
 * Created by mwei on 8/8/16.
 */
@Data
public class TokenResponse implements ICorfuPayload<TokenResponse>, IToken {

    public static byte[] NO_CONFLICT_KEY = new byte[]{};

    /**
     * Constructor for TokenResponse.
     *
     * @param tokenValue token value
     * @param epoch current epoch
     * @param backpointerMap  map of backpointers for all requested streams
     * @param streamsMap a map of queried stream tails
     */

    /** the cause/type of response. */
    final TokenType respType;

    /**
     * In case there is a conflict, signal to the client which key was responsible for the conflict.
     */
    final byte[] conflictKey;

    /** The current token,
     * or overload with "cause address" in case token request is denied. */
    final Token token;

    /** The backpointer map, if available. */
    final Map<UUID, Long> backpointerMap;

    /**
     * A tails map that contains the result for a multi-tail query.
     */
    final Map<UUID, Long> streamsMap;

    /**
     * Deserialization Constructor from a Bytebuf to TokenResponse.
     *
     * @param buf The buffer to deserialize
     */
    public TokenResponse(ByteBuf buf) {
        respType = TokenType.values()[ICorfuPayload.fromBuffer(buf, Byte.class)];
        conflictKey = ICorfuPayload.fromBuffer(buf, byte[].class);
        Long tokenValue = ICorfuPayload.fromBuffer(buf, Long.class);
        Long epoch = ICorfuPayload.fromBuffer(buf, Long.class);
        token = new Token(tokenValue, epoch);
        backpointerMap = ICorfuPayload.mapFromBuffer(buf, UUID.class, Long.class);
        streamsMap = ICorfuPayload.mapFromBuffer(buf, UUID.class, Long.class);
    }

    /**
     * A constructor to create a single stream query response
     * @param tokenValue token value
     * @param epoch sequencer's epoch when the token was generated
     * @param backpointerMap backpointers for the stream
     */
    public TokenResponse(long tokenValue, long epoch, Map<UUID, Long> backpointerMap) {
        this(TokenType.NORMAL, NO_CONFLICT_KEY, new Token(tokenValue, epoch), backpointerMap);
    }

    /**
     * A constructor to create a single token acquisition.
     *
     * @param respType type of response
     * @param conflictKey conflict keys
     * @param token actual token
     * @param backpointerMap backpointer map for the new global address.
     */
    public TokenResponse(TokenType respType, byte[] conflictKey, Token token, Map<UUID, Long> backpointerMap) {
        this(respType, conflictKey, token, backpointerMap, Collections.emptyMap());
    }

    /**
     *
     * Base constructor for a generic token response.
     *
     * @param respType type of response
     * @param conflictKey conflict key
     * @param token a token that contains an address and an epoch
     * @param backpointerMap map of backpointers for all requested streams
     * @param streamsMap map of stream tails (used when multiple streams are queried)
     */
    public TokenResponse(TokenType respType, byte[] conflictKey, Token token, Map<UUID, Long> backpointerMap,
                         Map<UUID, Long> streamsMap) {
        this.respType = respType;
        this.conflictKey = conflictKey;
        this.token = token;
        this.backpointerMap = backpointerMap;
        this.streamsMap = streamsMap;
    }

    @Override
    public void doSerialize(ByteBuf buf) {
        ICorfuPayload.serialize(buf, respType);
        ICorfuPayload.serialize(buf, conflictKey);
        ICorfuPayload.serialize(buf, token.getTokenValue());
        ICorfuPayload.serialize(buf, token.getEpoch());
        ICorfuPayload.serialize(buf, backpointerMap);
        ICorfuPayload.serialize(buf, streamsMap);
    }

    @Override
    public long getTokenValue() {
        return token.getTokenValue();
    }

    @Override
    public long getEpoch() {
        return token.getEpoch();
    }

}
