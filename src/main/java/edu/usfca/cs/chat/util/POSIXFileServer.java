package edu.usfca.cs.chat.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.attribute.PosixFilePermission;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.protobuf.ByteString;

import edu.usfca.cs.chat.ChatMessages;

import edu.usfca.cs.chat.net.ServerMessageRouter;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import ru.serce.jnrfuse.ErrorCodes;
import ru.serce.jnrfuse.struct.FileStat;

@ChannelHandler.Sharable
public class POSIXFileServer
    extends SimpleChannelInboundHandler<ChatMessages.ChatMessagesWrapper> {

    private static final Logger logger = Logger.getLogger("RemoteFS");

    ServerMessageRouter messageRouter;
    private String rootDir = ".";

    public POSIXFileServer() { }

    public void start(int port, String rootDir)
    throws IOException {
        this.rootDir = rootDir;
        messageRouter = new ServerMessageRouter(this);
        messageRouter.listen(port);
        System.out.println("File server listening on port " + port + "...");
        System.out.println("Serving files from " + this.rootDir);
    }

    public static void main(String[] args)
    throws IOException {
        POSIXFileServer s = new POSIXFileServer();
        s.start(Integer.parseInt(args[0]), args[1]);
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        /* A connection has been established */
        InetSocketAddress addr
            = (InetSocketAddress) ctx.channel().remoteAddress();
        System.out.println("Connection established: " + addr);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        /* A channel has been disconnected */
        InetSocketAddress addr
            = (InetSocketAddress) ctx.channel().remoteAddress();
        System.out.println("Connection terminated: " + addr);
    }

    @Override
    public void channelWritabilityChanged(ChannelHandlerContext ctx)
    throws Exception {
        /* Writable status of the channel changed */
    }

    @Override
    public void channelRead0(
            ChannelHandlerContext ctx, ChatMessages.ChatMessagesWrapper msg) {

        /* This method has some issues. First of all, the giant if-else block is
         * gross. More importantly, we're doing read() operations on the
         * channelRead thread, which can really be bad for performance. Ideally
         * This would be refactored as separate methods/threads and would use an
         * executor. */

        if (msg.hasReaddirReq() == true) {

            ChatMessages.ReaddirRequest readDir = msg.getReaddirReq();
            File reqPath = new File(rootDir + readDir.getPath());

            logger.log(Level.INFO, "readdir: {0}", reqPath);

            ChatMessages.ReaddirResponse.Builder respBuilder
                = ChatMessages.ReaddirResponse.newBuilder();
            respBuilder.setStatus(0);

            try {
                Files.newDirectoryStream(reqPath.toPath())
                    .forEach(path -> respBuilder.addContents(
                                path.getFileName().toString()));
            } catch (IOException e) {
                logger.log(Level.WARNING, "Error reading directory", e);
                respBuilder.setStatus(-ErrorCodes.ENOENT());
            }

            ChatMessages.ReaddirResponse resp = respBuilder.build();
            ChatMessages.ChatMessagesWrapper wrapper
                = ChatMessages.ChatMessagesWrapper.newBuilder()
                    .setReaddirResp(resp)
                .build();

            ctx.writeAndFlush(wrapper);

            logger.log(Level.INFO, "readdir status: {0}", resp.getStatus());

        } else if (msg.hasGetattrReq() == true) {

            ChatMessages.GetattrRequest getattr = msg.getGetattrReq();
            File reqPath = new File(rootDir + getattr.getPath());

            logger.log(Level.INFO, "getattr: {0}", reqPath);

            ChatMessages.GetattrResponse.Builder respBuilder
                = ChatMessages.GetattrResponse.newBuilder();
            respBuilder.setStatus(0);

            Set<PosixFilePermission> permissions = null;
            try {
                permissions = Files.getPosixFilePermissions(
                        reqPath.toPath(), LinkOption.NOFOLLOW_LINKS);
            } catch (IOException e) {
                logger.log(Level.WARNING, "Error reading file attributes", e);
                respBuilder.setStatus(-ErrorCodes.ENOENT());
            }

            int mode = 0;
            if (permissions != null) {
                for (PosixFilePermission perm : PosixFilePermission.values()) {
                    mode = mode << 1;
                    mode += permissions.contains(perm) ? 1 : 0;
                }
            }

            if (reqPath.isDirectory() == true) {
                mode = mode | FileStat.S_IFDIR;
            } else {
                mode = mode | FileStat.S_IFREG;
            }

            respBuilder.setSize(reqPath.length());
            respBuilder.setMode(mode);

            ChatMessages.GetattrResponse resp = respBuilder.build();

            ChatMessages.ChatMessagesWrapper wrapper
                = ChatMessages.ChatMessagesWrapper.newBuilder()
                    .setGetattrResp(resp)
                .build();

            ctx.writeAndFlush(wrapper);
            logger.log(Level.INFO, "getattr status: {0}", resp.getStatus());

        } else if (msg.hasOpenReq() == true) {

            ChatMessages.OpenRequest open = msg.getOpenReq();
            File reqPath = new File(rootDir + open.getPath());

            logger.log(Level.INFO, "open: {0}", reqPath);

            ChatMessages.OpenResponse.Builder respBuilder
                = ChatMessages.OpenResponse.newBuilder();
            respBuilder.setStatus(0);

            if (Files.isRegularFile(
                        reqPath.toPath(), LinkOption.NOFOLLOW_LINKS) == false) {
                respBuilder.setStatus(-ErrorCodes.ENOENT());
            }

            ChatMessages.OpenResponse resp = respBuilder.build();

            ChatMessages.ChatMessagesWrapper wrapper
                = ChatMessages.ChatMessagesWrapper.newBuilder()
                    .setOpenResp(resp)
                .build();

            ctx.writeAndFlush(wrapper);
            logger.log(Level.INFO, "open status: {0}", resp.getStatus());

        } else if (msg.hasReadReq() == true) {

            ChatMessages.ReadRequest read = msg.getReadReq();
            File reqPath = new File(rootDir + read.getPath());

            logger.log(Level.INFO, "read: {0}", reqPath);

            ChatMessages.ReadResponse.Builder respBuilder
                = ChatMessages.ReadResponse.newBuilder();

            try (FileInputStream fin = new FileInputStream(reqPath)) {
                fin.skip(read.getOffset());
                byte[] contents = new byte[(int) read.getSize()];
                int readSize = fin.read(contents);
                respBuilder.setContents(ByteString.copyFrom(contents));
                respBuilder.setStatus(readSize);
            } catch (IOException e) {
                logger.log(Level.WARNING, "Error reading file", e);
                respBuilder.setStatus(0);
            }

            ChatMessages.ReadResponse resp = respBuilder.build();

            ChatMessages.ChatMessagesWrapper wrapper
                = ChatMessages.ChatMessagesWrapper.newBuilder()
                    .setReadResp(resp)
                .build();

            ctx.writeAndFlush(wrapper);
            logger.log(Level.INFO, "read status: {0}", resp.getStatus());
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        cause.printStackTrace();
    }
}
