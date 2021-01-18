package edu.usfca.cs.chat.util;

//import com.sun.security.auth.module.UnixSystem;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.file.Paths;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.usfca.cs.chat.ChatMessages;
import jnr.ffi.Pointer;
import jnr.ffi.types.off_t;
import jnr.ffi.types.size_t;

import ru.serce.jnrfuse.FuseFillDir;
import ru.serce.jnrfuse.FuseStubFS;
import ru.serce.jnrfuse.struct.FileStat;
import ru.serce.jnrfuse.struct.FuseFileInfo;

public class POSIXClient extends FuseStubFS {

    private static final Logger logger = Logger.getLogger("RemoteFS");

    private final String hostname;
    private final int port;

    /**
     * Sends a blocking request to the file server using protobufs.
     *
     * This is an example of how to do "old" I/O in a way that will be
     * compatible with the LengthFieldPrepender + ProtobufEncoder Netty pipeline
     * we're using on the server side.
     * 
     * @param request protobuf message to send to the server
     * @return reply protobuf from the server
     * @throws IOException when there are issues reading from the socket
     */
    private ChatMessages.ChatMessagesWrapper sendRequest(
            ChatMessages.ChatMessagesWrapper request)
    throws IOException {

        Socket s = new Socket(hostname, port);
        DataOutputStream dos = new DataOutputStream(
                new BufferedOutputStream(s.getOutputStream()));
        byte[] payload = request.toByteArray();

        logger.log(Level.INFO, "Sending {0} byte request to {1}",
                new Object[] { payload.length, s });

        dos.writeInt(payload.length);
        dos.write(payload);
        dos.flush();

        DataInputStream dis = new DataInputStream(
                new BufferedInputStream(s.getInputStream()));
        int responseSize = dis.readInt();
        logger.log(Level.INFO, "Received {0} byte response", responseSize);

        byte[] response = new byte[responseSize];
        dis.read(response);

        ChatMessages.ChatMessagesWrapper respWrapper =
            ChatMessages.ChatMessagesWrapper.parseFrom(response);

        s.close();
        return respWrapper;
    }

    @Override
    public int readdir(
            String path, Pointer buf, FuseFillDir filler,
            @off_t long offset, FuseFileInfo fi) {

        logger.log(Level.INFO, "Reading directory: {0}", path);

        ChatMessages.ReaddirRequest req
            = ChatMessages.ReaddirRequest.newBuilder()
                .setPath(path)
            .build();

        ChatMessages.ChatMessagesWrapper reqWrapper
            = ChatMessages.ChatMessagesWrapper.newBuilder()
                .setReaddirReq(req)
            .build();

        ChatMessages.ChatMessagesWrapper respWrapper;
        try {
            respWrapper = sendRequest(reqWrapper);
        } catch (IOException e) {
            logger.log(Level.WARNING, "Failed to read directory", e);
            return -1;
        }

        if (respWrapper.hasReaddirResp() == false) {
            logger.warning("Received improper response type");
            return -1;
        }

        ChatMessages.ReaddirResponse rdr = respWrapper.getReaddirResp();
        if (rdr.getStatus() != 0) {
            /* Something went wrong on the server side */
            return rdr.getStatus();
        }

        /* Note that we aren't passing in a Statbuf (param 3). This doesn't seem
         * to really help much; the client is going to get a list of files and
         * then call getattr() on all of them anyway. */
        rdr.getContentsList().stream()
            .forEach(item -> filler.apply(buf, item, null, 0));

        /* These will always exist, but are filtered out on the server side: */
        filler.apply(buf, ".", null, 0);
        filler.apply(buf, "..", null, 0);

        return 0;
    }

    @Override
    public int open(String path, FuseFileInfo fi) {
        logger.log(Level.INFO, "Reading directory: {0}", path);

        ChatMessages.OpenRequest req
            = ChatMessages.OpenRequest.newBuilder()
                .setPath(path)
            .build();

        ChatMessages.ChatMessagesWrapper reqWrapper
            = ChatMessages.ChatMessagesWrapper.newBuilder()
                .setOpenReq(req)
            .build();

        ChatMessages.ChatMessagesWrapper respWrapper;
        try {
            respWrapper = sendRequest(reqWrapper);
        } catch (IOException e) {
            logger.log(Level.WARNING, "Failed to read directory", e);
            return -1;
        }

        if (respWrapper.hasOpenResp() == false) {
            logger.warning("Received improper response type");
            return -1;
        }

        return respWrapper.getOpenResp().getStatus();
    }

    @Override
    public int getattr(String path, FileStat stat) {

        logger.log(Level.INFO, "Retrieving file attributes: {0}", path);

        ChatMessages.GetattrRequest req
            = ChatMessages.GetattrRequest.newBuilder()
                .setPath(path)
            .build();

        ChatMessages.ChatMessagesWrapper reqWrapper
            = ChatMessages.ChatMessagesWrapper.newBuilder()
                .setGetattrReq(req)
            .build();

        ChatMessages.ChatMessagesWrapper respWrapper;
        try {
            respWrapper = sendRequest(reqWrapper);
        } catch (IOException e) {
            logger.log(Level.WARNING, "Failed to read file attributes", e);
            return -1;
        }

        if (respWrapper.hasGetattrResp() == false) {
            logger.warning("Received improper response type");
            return -1;
        }

        ChatMessages.GetattrResponse gar = respWrapper.getGetattrResp();
        if (gar.getStatus() != 0) {
            /* Something went wrong on the server side */
            return gar.getStatus();
        }

        stat.st_mode.set(gar.getMode());
        stat.st_size.set(gar.getSize());

        /* Okay, so this is kind of an ugly hack. If you're mounting files on a
         * remote machines, the UIDs aren't going to match up, so we lie here
         * and say that all files are owned by the user on the CLIENT machine.
         * It would probably be better to pass in the UID from an external
         * launcher script, or just set up permissions to be readable by
         * everyone, although that has its own issues... */
        long uid = 0L;//new UnixSystem().getUid();
        stat.st_uid.set(uid);

        return gar.getStatus();
    }

    @Override
    public int read(String path, Pointer buf,
            @size_t long size, @off_t long offset, FuseFileInfo fi) {

        logger.log(Level.INFO, "Read file: {0}; size = {1}, offset = {2}",
                new Object[] { path, size, offset});

        ChatMessages.ReadRequest req
            = ChatMessages.ReadRequest.newBuilder()
                .setPath(path)
                .setSize(size)
                .setOffset(offset)
            .build();

        ChatMessages.ChatMessagesWrapper reqWrapper
            = ChatMessages.ChatMessagesWrapper.newBuilder()
                .setReadReq(req)
            .build();

        ChatMessages.ChatMessagesWrapper respWrapper;
        try {
            respWrapper = sendRequest(reqWrapper);
        } catch (IOException e) {
            logger.log(Level.WARNING, "Failed to read file content", e);
            /* Note: for read(), 0 indicates failure */
            return 0;
        }

        if (respWrapper.hasReadResp() == false) {
            logger.warning("Received improper response type");
            /* Note: for read(), 0 indicates failure */
            return 0;
        }

        ChatMessages.ReadResponse rr = respWrapper.getReadResp();
        if (rr.getStatus() <= 0) {
            /* Something went wrong on the server side */
            return rr.getStatus();
        }

        byte[] contents = rr.getContents().toByteArray();
        logger.log(Level.INFO, "Read returned {0} bytes", contents.length);

        buf.put(0, contents, 0, contents.length);
        return contents.length;
    }


    public POSIXClient(String hostname, int port) {
        this.hostname = hostname;
        this.port = port;
    }

    public static void main(String[] args)
    throws IOException {
        if (args.length != 3) {
            System.err.println("Usage: ClientFS server port mount-point");
            System.exit(1);
        }
//        System.load("/usr/local/lib/libfuse/libfuse");
//        System.loadLibrary("/usr/local/lib/libfuse/libfuse");
        String hostname = args[0];
        int port = Integer.parseInt(args[1]);
        String mountPoint = args[2];

        POSIXClient fs = new POSIXClient(hostname, port);

        try { 
            logger.log(Level.INFO, "Mounting remote file system. "
                    + "Press Ctrl+C or send SIGINT to quit.");

            fs.mount(Paths.get(mountPoint), true, true);
        } finally {
            fs.umount();
        }
    }
}
