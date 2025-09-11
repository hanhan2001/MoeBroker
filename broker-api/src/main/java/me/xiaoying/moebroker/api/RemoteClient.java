package me.xiaoying.moebroker.api;

import io.netty.channel.Channel;
import me.xiaoying.moebroker.api.message.MessageHelper;
import me.xiaoying.moebroker.api.message.RequestMessage;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class RemoteClient implements Protocol{
    private final BrokerAddress address;

    private final Channel channel;

    public RemoteClient(BrokerAddress address, Channel channel) {
        this.address = address;
        this.channel = channel;
    }

    public BrokerAddress getAddress() {
        return this.address;
    }

    public Channel getChannel() {
        return this.channel;
    }

    @Override
    public void oneway(Object object) {
        this.channel.writeAndFlush(new RequestMessage(object));
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T invokeSync(Object object, long timeoutMillis) {
        CompletableFuture<Object> future = new CompletableFuture<>();

        RequestMessage message = new RequestMessage(object)
                .setChannel(this.channel)
                .setFuture(future)
                .setNeedResponse(true);

        MessageHelper.captureMessage(message, message.getChannel());

        this.channel.writeAndFlush(message);

        try {
            return (T) future.get(timeoutMillis, TimeUnit.MILLISECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw new RuntimeException(e);
        }
    }
}