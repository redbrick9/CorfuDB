package org.corfudb.protocols.wireprotocol;


import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandler;
import io.netty.channel.ChannelOutboundInvoker;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelPromise;
import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;


@Slf4j
public class FlushConsolidationHandler extends ChannelDuplexHandler {
    private volatile int explicitFlushAfterFlushes;
    private volatile int flushCount = 0;
    private long explicitFlushAfterMillis = 10;
    private final boolean consolidateWhenNoReadInProgress;
    private final Runnable flushTask;
    private volatile int flushPendingCount;
    private boolean readInProgress;
    private ChannelHandlerContext ctx;
    private Future<?> nextScheduledFlush;
    private volatile long lastFlushTime = -1;
    private long[] lastFlushes = new long[2];
    private int collectionNumber = 0;

    /**
     * Create new instance.
     *
     *
     */
    public FlushConsolidationHandler() {

        this.consolidateWhenNoReadInProgress = true;
        this.explicitFlushAfterFlushes = 256;
        flushTask = consolidateWhenNoReadInProgress ?
                new Runnable() {
                    @Override
                    public void run() {
                        if (flushPendingCount > 0 && !readInProgress) {
                            flushCount += flushPendingCount;
                            flushPendingCount = 0;
                            ctx.flush();
                            lastFlushTime = System.currentTimeMillis();
                            nextScheduledFlush = null;
                        } // else we'll flush when the read completes
                    }
                }
                : null;

    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
        this.ctx = ctx;
        lastFlushTime = System.currentTimeMillis();
        this.ctx.channel().eventLoop().scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                int tFlushCount = flushCount + flushPendingCount;
                if ((System.currentTimeMillis() - lastFlushTime > explicitFlushAfterMillis) && flushPendingCount > 0) {
                    flushNow(ctx);
                }
                lastFlushes[collectionNumber] = tFlushCount;
                collectionNumber = (collectionNumber + 1) % lastFlushes.length;
                flushCount = 0;
//                explicitFlushAfterFlushes = Math.min(10, Math.max(1,(int)(Arrays.stream(lastFlushes).sum() / lastFlushes.length)));

            }
        }, 0, explicitFlushAfterMillis, TimeUnit.MILLISECONDS);
    }

    @Override
    public void flush(ChannelHandlerContext ctx) throws Exception {
        if (++flushPendingCount == explicitFlushAfterFlushes) {
            flushNow(ctx);
        }

        if (readInProgress) {
            // If there is still a read in progress we are sure we will see a channelReadComplete(...) call. Thus
            // we only need to flush if we reach the explicitFlushAfterFlushes limit.
            if (++flushPendingCount == explicitFlushAfterFlushes) {
                flushNow(ctx);
            }
        } else if (consolidateWhenNoReadInProgress) {
            // Flush immediately if we reach the threshold, otherwise schedule
            if (++flushPendingCount == explicitFlushAfterFlushes) {
                flushNow(ctx);
            } else {
                scheduleFlush(ctx);
            }
        } else {
            // Always flush directly
            flushNow(ctx);
        }


    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
        // This may be the last event in the read loop, so flush now!
        resetReadAndFlushIfNeeded(ctx);
        ctx.fireChannelReadComplete();
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        readInProgress = true;
        ctx.fireChannelRead(msg);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        // To ensure we not miss to flush anything, do it now.
        resetReadAndFlushIfNeeded(ctx);
        ctx.fireExceptionCaught(cause);
    }

    @Override
    public void disconnect(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
        // Try to flush one last time if flushes are pending before disconnect the channel.
        resetReadAndFlushIfNeeded(ctx);
        ctx.disconnect(promise);
    }

    @Override
    public void close(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
        // Try to flush one last time if flushes are pending before close the channel.
        resetReadAndFlushIfNeeded(ctx);
        ctx.close(promise);
    }

    @Override
    public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception {
        if (!ctx.channel().isWritable()) {
            // The writability of the channel changed to false, so flush all consolidated flushes now to free up memory.
            flushIfNeeded(ctx);
        }
        ctx.fireChannelWritabilityChanged();
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
        flushIfNeeded(ctx);
    }

    private void resetReadAndFlushIfNeeded(ChannelHandlerContext ctx) {
        readInProgress = false;
        flushIfNeeded(ctx);
    }

    private void flushIfNeeded(ChannelHandlerContext ctx) {
        if (flushPendingCount > 0) {
            flushNow(ctx);
        }
    }

    private void flushNow(ChannelHandlerContext ctx) {
        cancelScheduledFlush();
        flushCount += flushPendingCount;
        flushPendingCount = 0;
        ctx.flush();
        lastFlushTime = System.currentTimeMillis();

    }

    private void scheduleFlush(final ChannelHandlerContext ctx) {
        if (nextScheduledFlush == null) {
            // Run as soon as possible, but still yield to give a chance for additional writes to enqueue.
            nextScheduledFlush = ctx.channel().eventLoop().submit(flushTask);
        }
    }

    private void cancelScheduledFlush() {
        if (nextScheduledFlush != null) {
            nextScheduledFlush.cancel(false);
            nextScheduledFlush = null;
        }
    }
}

