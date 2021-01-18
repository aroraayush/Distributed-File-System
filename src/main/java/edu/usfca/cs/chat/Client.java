package edu.usfca.cs.chat;

import com.google.protobuf.ByteString;
import com.sun.security.auth.module.UnixSystem;
import edu.usfca.cs.chat.net.MessagePipeline;
import edu.usfca.cs.chat.util.Directory;
import edu.usfca.cs.chat.util.ReadConfig;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import jnr.ffi.Pointer;
import jnr.ffi.types.off_t;
import jnr.ffi.types.size_t;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.serce.jnrfuse.FuseFillDir;
import ru.serce.jnrfuse.FuseStubFS;
import ru.serce.jnrfuse.struct.FileStat;
import ru.serce.jnrfuse.struct.FuseFileInfo;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;

@ChannelHandler.Sharable
public class Client
    extends SimpleChannelInboundHandler<ChatMessages.ChatMessagesWrapper> {

    private Logger logger = LoggerFactory.getLogger(Client.class.getName());

    private String filePath;
    private Bootstrap bootstrap;
    private ExecutorService executorService;
    private Channel serverChannel;
    private Channel storageNodeChannel;
    private EventLoopGroup workerGroup;
    private MessagePipeline pipeline;
    private String mountPoint;
    private volatile int chunkCount;
    private volatile int currentChunkCount;
    private int sizeOfChunk; // 10MB
    private ArrayList<POSIXClient> posixClients;

    public void setFilePath(String filePath) {
        this.filePath = filePath;
    }

    public Client(int sizeOfChunk, String mountPoint) {
        this.executorService = Executors.newCachedThreadPool();
        this.chunkCount = 0;
        this.currentChunkCount = 0;
        this.sizeOfChunk = sizeOfChunk;
        this.posixClients = new ArrayList<>();
        this.mountPoint = mountPoint;
    }

    public static void main(String[] args) throws IOException {
        ReadConfig config = new ReadConfig();
        Client c = new Client(config.getChunkSize(), config.getMountPoint());

        try {

            c.createBootstrap();
            c.serverChannel = c.connectChannel(config.getControllerHost(),config.getControllerPort());
            System.out.println("Connected to client");
            c.fetchFile();



        } catch (IOException | InterruptedException e) {
            c.logger.error("Unable to fetch controller hostname, port or bloom filter size");
            c.logger.error("Exiting...");
            System.exit(1);
        }

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            c.logger.info("Shutting down the client executor service");
            //shutdown executor service here
            c.executorService.shutdownNow();
        }));

    }

    private void fetchFile() throws IOException{
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        System.out.println("Enter your choice : \n1-Write a File\n2-Read a File\n3-Exit");
        String ch = br.readLine();
        this.currentChunkCount = 0;
        this.chunkCount = 0;
        if(ch.equals("1")){
            //System.out.println("Enter the directory Name you want to write : ");
        //                String dirName = br.readLine();
            this.filePath = "/Users/adarsh/IdeaProjects/P1-adarsh-and-ayush/proto/temp.mp4";
            File f = new File(this.filePath);
            if(f.isDirectory()){
                sendDirectoryMessage();
            }
            else {
                sendFileMessage(this.filePath, 1);
            }
        }
        else if(ch.equals("2")){
            System.out.println("Enter the file Name you want to fetch : ");
        //                String fileName = br.readLine();
            logger.info("Requesting file from server...");
            String fileName = "temp.mp4";
            for(POSIXClient client : this.posixClients){
                client.umount();
            }
            this.posixClients.clear();
            filePath = fileName;
            sendFileMessage(fileName, 2);
        }
        else if(ch.equals("3")){
            System.out.println("Thank you, bye!");
            System.exit(1);
        }
        else{
            System.out.println("Please enter a valid choice ");
            fetchFile();
        }

    }

    private void sendFileMessage(String fileName, int requestType){
        double size;

        File file = new File(fileName);
        // We store only the directory for the files (parent path)
        System.out.println("File Len : "+file.length());
        if(requestType == 1)  // write request
        {
            size = (double) (file.length() / (1024 * 1024));
            this.filePath = this.filePath.substring(0,this.filePath.length()-file.getName().length() - 1);
        }
        else {
            size = -1;      // read request
        }
        ChatMessages.FileInfo fInfo = ChatMessages.FileInfo.newBuilder()
                .setFileName(file.getName())
                .setSize(size)
                .build();

        ChatMessages.ChatMessagesWrapper msgWrapper = ChatMessages.ChatMessagesWrapper.newBuilder()
                .setFInfo(fInfo)
                .build();
        // build directory as well
        ChannelFuture write = serverChannel.writeAndFlush(msgWrapper);
        write.syncUninterruptibly();
    }

    private void sendDirectoryMessage() {
        File dir = new File(this.filePath);
        String contents[] = dir.list();
        for(int i=0; i<contents.length; i++) {
            File file = new File(this.filePath, contents[i]);
            if(!file.isDirectory())
                sendFileMessage(file.getAbsolutePath(),1);
        }
    }

//    private String createDirBuffer(File rootDir, String fileName, String path){
//        File f = new File(path);
//        if(!f.isDirectory() && rootDir.getName().equals(fileName))
//            return path;
//        else if(f.isDirectory()){
//            for(int x = 0;x < rootDir.getChildren().length;x++){
//                return directoryRead(rootDir.getChildren()[x], fileName, path + "/" + rootDir.getChildren()[x].getName());
//            }
//        }
//        return path;
//    }

    private String directoryRead(Directory rootDir, String fileName, String path){
        File f = new File(path);
        if(!f.isDirectory() && rootDir.getName().equals(fileName))
            return path;
        else if(f.isDirectory()){
            for(int x = 0;x < rootDir.getChildren().size();x++){
                return directoryRead(rootDir.getChildren().get(x), fileName, path + "/" + rootDir.getChildren().get(x).getName());
            }
        }
        return path;
    }

    // Read a file
        // Break into chunks
        // Get the location from controller
        // Write to storage node, based on channelhandler writer
        //if file unfinished, get next storageNode from controller :repeat

    private void readSplitSendFile(List<String> hostPort, String filename) throws IOException {

       int countChunk = 1;
       int offset = 0;


       File file = new File(this.filePath, filename);
       long fSize = file.length();

        ArrayList<String> hostPortUpdated = new ArrayList<>();
        for(int i = 1; i< hostPort.size(); i++){
            hostPortUpdated.add(hostPort.get(i));
        }
       try(FileInputStream fs = new FileInputStream(file);
           BufferedInputStream bs = new BufferedInputStream(fs)){
           int bytesBuffer = 0;
           while(offset * this.sizeOfChunk < fSize){
               byte[] buffer = new byte[sizeOfChunk];
               String chunkName = filename + "_" + countChunk;
               String[] reg = hostPort.get(countChunk - 1).split(":");
               countChunk++;

               //bs.skip(offset*this.sizeOfChunk);
               System.out.println("offset*this.sizeOfChunk "+ offset*this.sizeOfChunk);
              if(offset == (fSize / this.sizeOfChunk)){
                  System.out.println("In if");
                  bytesBuffer = bs.read(buffer, 0, (int)fSize - this.sizeOfChunk*offset);
                  System.out.println(ByteString.copyFrom(buffer));
                  System.out.println("================================================================================="+bytesBuffer);


                  Runnable r4 = new SendChunks(reg[0], Integer.parseInt(reg[1]), chunkName, buffer, bytesBuffer, hostPortUpdated);
                  executorService.execute(r4);
              }
              else {
                  System.out.println("In else ");
                  bytesBuffer = bs.read(buffer, 0, this.sizeOfChunk);
                  System.out.println(ByteString.copyFrom(buffer));
                  System.out.println("================================================================================="+bytesBuffer);

                  Runnable r4 = new SendChunks(reg[0], Integer.parseInt(reg[1]), chunkName, buffer, bytesBuffer, hostPortUpdated);
                  executorService.execute(r4);
              }


//               File newFile = new File(file.getParent(), chunkName);
////               try (FileOutputStream out = new FileOutputStream(newFile)) {
//                   out.write(buffer, 0, bytesBuffer);
//               }
//
//               System.out.println(buffer);




               offset++;
           }
       }
       catch (Exception e){
           logger.error(String.valueOf(e));
       }
       finally {
           try {
               logger.info("Sending file to server...");
               Thread.sleep(1000);
               logger.info("File sent!!");
               this.fetchFile();
           } catch (IOException | InterruptedException e) {
               logger.error("Error : " +e.getMessage());
           }
       }
    }

    private class SendChunks implements Runnable  {
        private String hostName;
        private int port;
        private String chunkName;
        private byte[] buffer;
        private List<String> chunksHostPort;
        private int chunkSize;

        public SendChunks(String hostName, int port, String chunkName, byte[] buffer, int chunkSize, List<String> chunksHostPort) throws Exception{
            this.hostName = hostName;
            this.port = port;
            this.chunkName = chunkName;
            this.buffer = buffer;
            this.chunksHostPort = chunksHostPort;
            this.chunkSize = chunkSize;
        }
        public void run() {

            Channel channel = null;
            try {
                channel = connectChannel(hostName, port);
            } catch (InterruptedException e) {
                logger.error(e.getMessage());
            }

            ChatMessages.FileData.Builder fileData = ChatMessages.FileData.newBuilder()
                    .setFileName(chunkName)
                    .setFileBuffer(ByteString.copyFrom(buffer))
                    .setFsize(chunkSize)
                    .setSource(true);
            int chunkLen = chunkName.length();
            if (chunkName.charAt(chunkLen - 1) == '1' && chunkName.charAt(chunkLen - 2) == '_') {
                // send host ports List to storage Node with chunk_1
                fileData.setSourceHost(String.join(",", chunksHostPort));
            }

            ChatMessages.ChatMessagesWrapper msgWrapper = ChatMessages.ChatMessagesWrapper.newBuilder()
                    .setFileBuf(fileData.build())
                    .build();

            ChannelFuture write = channel.writeAndFlush(msgWrapper);
            write.syncUninterruptibly();
        }
    }

    public void createBootstrap() {
        workerGroup = new NioEventLoopGroup();
        pipeline = new MessagePipeline(this);

        bootstrap = new Bootstrap()
            .group(workerGroup)
            .channel(NioSocketChannel.class)
            .option(ChannelOption.SO_KEEPALIVE, true)
            .handler(pipeline);
    }

    class ConnectChannel implements Runnable{
        private String hostName;
        private int port;
        private ChannelFuture cf;
        private Client c;

        public ConnectChannel(String hostName, int port, Client c) {
            this.hostName = hostName;
            this.port = port;
            this.c = c;
        }

        @Override
        public void run(){
            System.out.println("Connecting to " + hostName + ":" + port);
            Bootstrap bootstrap1 = new Bootstrap()
                    .group(new NioEventLoopGroup())
                    .channel(NioSocketChannel.class)
                    .option(ChannelOption.SO_KEEPALIVE, true)
                    .handler(new MessagePipeline(c));
            cf = bootstrap1.connect(hostName, port);
//            cf = bootstrap.connect(hostName, port);
            cf.syncUninterruptibly();
        }

        public ChannelFuture getCf() {
            return cf;
        }
    }

    private Channel connectChannel(String hostname, int port) throws InterruptedException {
        ConnectChannel connectChannel = new ConnectChannel(hostname, port,this);
        Thread thread = new Thread(connectChannel);
        thread.start();
        thread.join();
        ChannelFuture cf = connectChannel.getCf();
//        System.out.println("Connecting to " + hostname + ":" + port);
//        ChannelFuture cf = bootstrap.connect(hostname, port);
//        cf.syncUninterruptibly();
        return cf.channel();
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
        System.out.println("Connection lost: " + addr);
    }

    @Override
    public void channelWritabilityChanged(ChannelHandlerContext ctx)
    throws Exception {
        /* Writable status of the channel changed */
    }

    private class SendReadRequest implements Runnable{
        private Channel ch;
        private String fileName;

        SendReadRequest(Channel ch, String fileName){
            this.ch = ch;
            this.fileName = fileName;
        }
        public void run() {
            ChatMessages.DirectoryMsg directoryMsg = ChatMessages.DirectoryMsg.newBuilder()
                    .setName(fileName)
                    .build();
            ChatMessages.ChatMessagesWrapper msgWrapper = ChatMessages.ChatMessagesWrapper.newBuilder()
                    .setDir(directoryMsg)
                    .build();
            ChannelFuture write = this.ch.writeAndFlush(msgWrapper);
            write.syncUninterruptibly();
        }
    }

    @Override
    public void channelRead0(
            ChannelHandlerContext ctx, ChatMessages.ChatMessagesWrapper msg) throws IOException, InterruptedException {

        if(msg.hasSMeta()){
            if(msg.getSMeta().getRequestType() == ChatMessages.StorageNodeMetaData.RequestType.WRITE) {   // true - write to SN
                List<String> optimalNodes = msg.getSMeta().getHostPortList();
                if(optimalNodes.size()>0){
                    readSplitSendFile(optimalNodes, msg.getSMeta().getFile().getFileName());
                }
                else {
                    logger.info("Not enough nodes to store file");
                    this.fetchFile();
                }
            }
            else if(msg.getSMeta().getRequestType() == ChatMessages.StorageNodeMetaData.RequestType.READ) {      // read from SN (Received from controller)

                if(msg.getSMeta().getHostPortList().size() == 0){
                    logger.info("File "+ msg.getSMeta().getFile().getFileName() +  " not found");
                    this.fetchFile();
                }
                else {
                    // read the file

                    String hostPort = msg.getSMeta().getHostPort(0);
                    String[] reg = hostPort.split(":");
                    String snHost = reg[0];
                    int snPort = Integer.parseInt(reg[1]);
                    String fileName = msg.getSMeta().getFile().getFileName()+"_1";
                    if(reg.length > 2){
                        fileName = reg[2] + "/" + fileName;
                    }

//                POSIXClient posixClient = new POSIXClient(snHost, snPort, this.mountPoint+"1");
//                this.posixClients.add(posixClient);
//                posixClient.mount(Paths.get(mountPoint+"1"), true, true);


                    Channel ch = connectChannel(snHost, snPort);
                    Runnable r = new SendReadRequest(ch, fileName);
                    executorService.execute(r);
                }
            }
            else if(msg.getSMeta().getRequestType() == ChatMessages.StorageNodeMetaData.RequestType.REPLICA){
                String fileName = msg.getSMeta().getFile().getFileName();
                File f = new File(this.filePath);
                List<String> replicas = msg.getSMeta().getHostPortList();
                if(fileName.equals(f.getName().substring(0,f.getName().length()-2)) && replicas.size() > 0){
                    String[] hostPort = replicas.get(0).split(":");
                    Channel ch = connectChannel(hostPort[0], Integer.parseInt(hostPort[1]));

                    Runnable r1 = new SendReadRequest(ch, msg.getSMeta().getFile().getFileName());
                    executorService.execute(r1);
                }
            }
        }
        else if(msg.hasFileBuf()){
            String dirCurrent = Paths.get("").toAbsolutePath().toString();
            String fileName = msg.getFileBuf().getFileName();

            String sourceHostPorts = msg.getFileBuf().getSourceHost();
            String[] sourceHost = sourceHostPorts.split(",");

            if(fileName.charAt(fileName.length() - 1) == '1' && fileName.charAt(fileName.length() - 2) == '_' && sourceHost[0].length() >= 1) {
                this.chunkCount = sourceHost.length +1;
                requestControllerNodeFailCheck(fileName.substring(0,fileName.length()-2), sourceHostPorts);
            }
            else if(fileName.charAt(fileName.length() - 1) == '1' && fileName.charAt(fileName.length() - 2) == '_'){
                this.chunkCount = 1;
            }

            Runnable r5 = new WriteChunksOnClient(msg, fileName, dirCurrent, this);
            executorService.execute(r5);
        }
        else if(msg.hasRepStatus()){ // rest of chunks updated list from controller
            String sourceHostPorts = msg.getRepStatus().getSourceHost();
            String[] sourceHost = sourceHostPorts.split(",");

            try {
                for (int x = 0; x < sourceHost.length; x++) {
                    String[] reg = sourceHost[x].split(":");
                    String snHost = reg[0];
                    int snPort = Integer.parseInt(reg[1]);
                    String chunkName = msg.getRepStatus().getFilename() + "_"+ (x+2);
                    if(reg.length > 2){
                        chunkName = reg[2] + "/" + chunkName;
                    }
                    Channel ch =connectChannel(snHost, snPort);

                    //logger.info("mountPoint :"+ this.mountPoint+(snPort % 10));
//                    POSIXClient posixClient = new POSIXClient(snHost, snPort, this.mountPoint+(snPort % 10));
//                    this.posixClients.add(posixClient);
//                    posixClient.mount(Paths.get(mountPoint+(snPort % 10)), true, true);

                    Runnable r2 = new SendReadRequest(ch, chunkName);
                    executorService.execute(r2);
                }
            }
            catch (Exception e){
                e.printStackTrace();
            }

        }
        //call fileTransfer from here
    }

    private class WriteChunksOnClient implements Runnable{
        private ChatMessages.ChatMessagesWrapper msg;
        private String fileName;
        private String dirCurrent;
        private Client c;

        public WriteChunksOnClient(ChatMessages.ChatMessagesWrapper msg, String fileName, String dirCurrent, Client c) {
            this.msg = msg;
            this.fileName = fileName;
            this.dirCurrent = dirCurrent;
            this.c = c;
        }

        @Override
        public void run(){
            logger.info("The requested file "+fileName+" copied to :" + dirCurrent + " with size :"+msg.getFileBuf().getFsize());
            byte[] buffer = msg.getFileBuf().getFileBuffer().toByteArray();
            File file = new File(dirCurrent, fileName);
            try(FileOutputStream fs = new FileOutputStream(file)){
                fs.write(buffer,0, msg.getFileBuf().getFsize()); //TODO append fileSize > original size

                c.currentChunkCount++;
            }
            catch (Exception e){
                logger.error(e.getMessage());
            }
            if(c.currentChunkCount == c.chunkCount){
                logger.info("In merge chunks");
                mergeFile(fileName, c.chunkCount);
                try {

                    c.fetchFile();
                } catch (IOException e) {
                    logger.error("Error taking user input again :" + e.getMessage());
                }
            }
        }
    }

    private void requestControllerNodeFailCheck(String fileName, String sourceHostPorts) {

        ChatMessages.ReplicaStatus replicaStatus = ChatMessages.ReplicaStatus.newBuilder()
                .setFilename(fileName)
                .setSourceHost(sourceHostPorts)
                .build();

        ChatMessages.ChatMessagesWrapper msgWrapper = ChatMessages.ChatMessagesWrapper.newBuilder()
                .setRepStatus(replicaStatus)
                .build();

        ChannelFuture write = serverChannel.writeAndFlush(msgWrapper);
        write.syncUninterruptibly();
    }

    private void mergeFile(String fileName, int chunkCount) {

        String fName = fileName.substring(0,fileName.lastIndexOf('_'));
        try (FileOutputStream fos = new FileOutputStream(new File(fName));
             BufferedOutputStream mergingStream = new BufferedOutputStream(fos)) {
            for(int x = 1;x <= chunkCount;x++){
                File f = new File(fName + "_" + x);
                Files.copy(f.toPath(), mergingStream);
            }
        } catch (FileNotFoundException e) {
            logger.error("Directory to merge file not found ");
        } catch (IOException e) {
            logger.error(e.getMessage());
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        cause.printStackTrace();
    }

    private static class POSIXClient extends FuseStubFS {

        private static final java.util.logging.Logger logger = java.util.logging.Logger.getLogger(POSIXClient.class.getName());

        private final String hostname;
        private final int port;
        private final String mountPoint;

        public POSIXClient(String hostname, int port, String mountPoint) {
            this.hostname = hostname;
            this.port = port;
            this.mountPoint = mountPoint;
        }

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
            long uid = new UnixSystem().getUid();
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
    }

}
