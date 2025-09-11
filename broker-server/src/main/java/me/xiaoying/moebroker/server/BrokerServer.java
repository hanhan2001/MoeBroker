package me.xiaoying.moebroker.server;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import me.xiaoying.moebroker.api.BrokerAddress;
import me.xiaoying.moebroker.api.Protocol;
import me.xiaoying.moebroker.api.RemoteClient;
import me.xiaoying.moebroker.api.executor.ExecutorManager;
import me.xiaoying.moebroker.api.message.MessageHelper;
import me.xiaoying.moebroker.api.message.RequestMessage;
import me.xiaoying.moebroker.api.message.close.CloseRequestMessage;
import me.xiaoying.moebroker.api.message.heartbeat.HeartbeatPingMessage;
import me.xiaoying.moebroker.api.message.heartbeat.HeartbeatProcessor;
import me.xiaoying.moebroker.api.netty.SerializableDecoder;
import me.xiaoying.moebroker.api.netty.SerializableEncoder;
import me.xiaoying.moebroker.api.processor.ProcessorManager;
import me.xiaoying.moebroker.api.service.InvokeMethodMessageProcessor;
import me.xiaoying.moebroker.server.netty.ConnectionHandler;
import me.xiaoying.moebroker.server.netty.MessageHandler;
import me.xiaoying.moebroker.server.processor.CloseRequestMessageProcessor;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

public abstract class BrokerServer implements Protocol {
    private final BrokerAddress address;

    protected volatile boolean running = false;

    protected ChannelFuture channelFuture;

    protected EventLoopGroup bossGroup;

    protected EventLoopGroup workerGroup;

    protected ProcessorManager processorManager;

    protected final List<RemoteClient> clients = new ArrayList<>();

    protected final List<ScheduledFuture<?>> scheduledFutures = new ArrayList<>();

    public BrokerServer(BrokerAddress address) {
        this.address = address;

        this.processorManager = new ProcessorManager();
        this.processorManager.registerProcessor(new HeartbeatProcessor());
        this.processorManager.registerProcessor(new InvokeMethodMessageProcessor());
        this.processorManager.registerProcessor(new CloseRequestMessageProcessor());
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

    protected void start() {
        this.workerGroup = new NioEventLoopGroup();
        this.bossGroup = new NioEventLoopGroup();

        ServerBootstrap bootstrap = new ServerBootstrap();

        bootstrap.group(this.bossGroup, this.workerGroup)
                .channel(NioServerSocketChannel.class)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) throws Exception {
                        ch.pipeline()
                                .addLast(new SerializableEncoder())
                                .addLast(new SerializableDecoder())
                                .addLast(new MessageHandler(BrokerServer.this))
                                .addLast(new ConnectionHandler(BrokerServer.this));
                    }
                });

        this.channelFuture = bootstrap.bind(this.address.getHost(), this.address.getPort()).syncUninterruptibly();
        this.channelFuture.addListener((ChannelFutureListener) future -> {
            if (!future.isSuccess())
                return;

            this.running = true;
            ExecutorManager.getExecutor("broker").execute(this::onStart);
            this.scheduledFutures.add(ExecutorManager.getScheduledExecutor("heartbeat").scheduleWithFixedDelay(() -> this.invokeSync(new HeartbeatPingMessage()), 30, 30, TimeUnit.SECONDS));
        });

        try {
            this.channelFuture.sync();
            this.channelFuture.channel().closeFuture().sync();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            this.bossGroup.shutdownGracefully();
            this.workerGroup.shutdownGracefully();
        }
    }

    public BrokerAddress getAddress() {
        return this.address;
    }

    public ProcessorManager getProcessorManager() {
        return this.processorManager;
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
        this.clients.forEach(client -> {
            // 因为 close 可能代表 Server 将要强制关闭，所以需要处理返回内容
            client.oneway(new CloseRequestMessage("stop", System.currentTimeMillis()));

            client.getChannel().close();
        });

        this.running = false;

        this.scheduledFutures.forEach(scheduledFuture -> scheduledFuture.cancel(true));
    }

    public void captureClient(RemoteClient client) {
        this.clients.add(client);
    }

    /**
     * 当 server 启动时会调用此方法
     */
    public abstract void onStart();

    /**
     * 获取新请求时会调用此方法
     */
    public abstract void onOpen(RemoteClient remote);

    /**
     * 连接关闭时会调用此方法
     */
    public abstract void onClose(RemoteClient remote);

    /**
     * 报错触发方法
     */
    public void onError(RemoteClient remote, Throwable cause) {
        if (!remote.getChannel().isActive()) {
            this.onClose(remote);
            return;
        }

        cause.printStackTrace();
    }
}