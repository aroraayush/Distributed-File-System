package edu.usfca.cs.chat.net;

import edu.usfca.cs.chat.ChatMessages;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.codec.protobuf.ProtobufDecoder;
import io.netty.handler.codec.protobuf.ProtobufEncoder;
import io.netty.handler.stream.ChunkedWriteHandler;

public class MessagePipeline extends ChannelInitializer<NioSocketChannel> {

    private ChannelInboundHandlerAdapter inboundHandler;

    public MessagePipeline(ChannelInboundHandlerAdapter inboundHandler) {
        this.inboundHandler = inboundHandler;
    }

    @Override
    public void initChannel(NioSocketChannel ch) throws Exception {
        ChannelPipeline pipeline = ch.pipeline();

        /* Here, we limit message sizes to 8192: */
        pipeline.addLast(new LengthFieldBasedFrameDecoder(1024*1024*11, 0, 4, 0, 4));
        pipeline.addLast(
                new ProtobufDecoder(
                    ChatMessages.ChatMessagesWrapper.getDefaultInstance()));

        pipeline.addLast(new LengthFieldPrepender(4));
        pipeline.addLast(new ProtobufEncoder());
        pipeline.addLast(new ChunkedWriteHandler());
        pipeline.addLast(inboundHandler);
    }
}
