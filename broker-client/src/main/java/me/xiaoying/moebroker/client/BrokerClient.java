package me.xiaoying.moebroker.client;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import me.xiaoying.moebroker.api.BrokerAddress;
import me.xiaoying.moebroker.api.Protocol;
import me.xiaoying.moebroker.api.executor.ExecutorManager;
import me.xiaoying.moebroker.api.message.MessageHelper;
import me.xiaoying.moebroker.api.message.RequestMessage;
import me.xiaoying.moebroker.api.message.close.CloseRequestMessage;
import me.xiaoying.moebroker.api.message.close.CloseResponseMessage;
import me.xiaoying.moebroker.api.message.heartbeat.HeartbeatProcessor;
import me.xiaoying.moebroker.api.netty.SerializableDecoder;
import me.xiaoying.moebroker.api.netty.SerializableEncoder;
import me.xiaoying.moebroker.api.processor.ProcessorManager;
import me.xiaoying.moebroker.api.service.InvokeMethodMessageProcessor;
import me.xiaoying.moebroker.client.netty.ClientMessageHandler;
import me.xiaoying.moebroker.client.netty.ConnectionHandler;
import me.xiaoying.moebroker.client.processor.CloseRequestMessageProcessor;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public abstract class BrokerClient implements Protocol {
    protected final ProcessorManager processorManager;

    protected volatile boolean running = false;

    protected final BrokerAddress address;

    protected ChannelFuture channelFuture;

    protected EventLoopGroup bossGroup;

    protected DefaultEventExecutorGroup workerGroup;

    public BrokerClient(final BrokerAddress address) {
        this.address = address;

        this.processorManager = new ProcessorManager();
        this.processorManager.registerProcessor(new HeartbeatProcessor());
        this.processorManager.registerProcessor(new CloseRequestMessageProcessor());
        this.processorManager.registerProcessor(new InvokeMethodMessageProcessor());
    }

    public void run() {
        ExecutorManager.getExecutor("broker").execute(this::start);

        while (!this.running) {
            try {
                Thread.sleep(20);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private void start() {
        this.bossGroup = new NioEventLoopGroup();
        this.workerGroup = new DefaultEventExecutorGroup(8);

        try {
            Bootstrap bootstrap = new Bootstrap();
            bootstrap.group(this.bossGroup)
                    .channel(NioSocketChannel.class)
                    .handler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) throws Exception {
                            ch.pipeline()
                                    .addLast(new SerializableEncoder())
                                    .addLast(new SerializableDecoder())
                                    .addLast(new ClientMessageHandler(BrokerClient.this))
                                    .addLast(BrokerClient.this.workerGroup, "clientHandler", new ConnectionHandler(BrokerClient.this));
                        }
                    });

            this.channelFuture = bootstrap.connect(this.address.getHost(), this.address.getPort());
            ExecutorManager.getExecutor("broker").execute(this::onStart);

            this.channelFuture.addListener((ChannelFuture future) -> {
                if (!future.isSuccess())
                    return;

                this.running = true;
                ExecutorManager.getExecutor("broker").execute(this::onOpen);
            });

            this.channelFuture.channel().closeFuture().addListener((ChannelFuture future) -> ExecutorManager.getExecutor("broker").execute(this::onClose));

            this.channelFuture.sync();
            this.channelFuture.channel().closeFuture().sync();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            this.bossGroup.close();
        }
    }

    @Override
    public void oneway(Object object) {
        this.channelFuture.channel().writeAndFlush(new RequestMessage(object));
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T invokeSync(Object object, long timeoutMillis) {
        CompletableFuture<Object> future = new CompletableFuture<>();

        RequestMessage message = new RequestMessage(object)
                .setChannel(this.channelFuture.channel())
                .setFuture(future)
                .setNeedResponse(true);

        MessageHelper.captureMessage(message, message.getChannel());

        this.channelFuture.channel().writeAndFlush(message);

        try {
            return (T) future.get(timeoutMillis, TimeUnit.MILLISECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw new RuntimeException(e);
        }
    }

    public void close() {
        CloseResponseMessage response = (CloseResponseMessage) this.invokeSync(new CloseRequestMessage("normal", System.currentTimeMillis()));
        if (!response.isAccepted())
            return;

        this.running = false;

        this.channelFuture.channel().close();
    }

    public BrokerAddress getAddress() {
        return this.address;
    }

    public ProcessorManager getProcessorManager() {
        return this.processorManager;
    }

    public abstract void onStart();

    public abstract void onOpen();

    public abstract void onClose();

    public abstract void onError(Throwable cause);
}