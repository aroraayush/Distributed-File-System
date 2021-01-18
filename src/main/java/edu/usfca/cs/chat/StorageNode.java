package edu.usfca.cs.chat;

import com.google.protobuf.ByteString;
import edu.usfca.cs.chat.net.MessagePipeline;
import edu.usfca.cs.chat.net.ServerMessageRouter;
import edu.usfca.cs.chat.util.ReadConfig;
import edu.usfca.cs.chat.util.StorageNodeUtils;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.serce.jnrfuse.ErrorCodes;
import ru.serce.jnrfuse.struct.FileStat;

import java.io.*;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.attribute.PosixFilePermission;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.zip.Adler32;
import java.util.zip.Checksum;

@ChannelHandler.Sharable
public class StorageNode extends SimpleChannelInboundHandler<ChatMessages.ChatMessagesWrapper> {
    //TODO: under replication(pq) or over replication manage

    private ExecutorService executorService;
    //private ChatMessages.NodeState state;
    private int term;
    private int voteCount;


    private Logger logger = LoggerFactory.getLogger(StorageNode.class.getName());
    private HashMap<String, Long> checksumMap;
    private int listeningPort;
    private int contrPort;
    private int requestsProcessed;
    private String rootDirPath;
    private String contrAddress;
    private Bootstrap bootstrap;
    private double spaceAvailable;
    private Channel controllerChannel;
    private ArrayList<String> replicasList;
    private ServerMessageRouter messageRouter;
    private HashMap<String,String> chunkLocations;
    private int chunkSize;
    private String rootDir;

    public StorageNode() {
        try {
            ReadConfig config = new ReadConfig();
            this.rootDirPath = config.getsNDirPath();
            this.executorService = Executors.newCachedThreadPool();
            this.contrAddress = config.getControllerHost();
            this.contrPort = config.getControllerPort();
            this.spaceAvailable = StorageNodeUtils.getAvailableSpace();
            this.listeningPort = -1;
            this.requestsProcessed = 0;
            this.replicasList = new ArrayList<>();
            this.chunkLocations = new HashMap<>();
            this.checksumMap = new HashMap<>();
            this.chunkSize = config.getChunkSize();
            this.voteCount = 0;
//            this.replicas = new StorageNode[config.getReplicationFactor()];
            //this.state = ChatMessages.NodeState.FOLLOWER;

        } catch (IOException e) {
            logger.error("Unable to fetch controller hostname, port or bloom filter size");
            logger.error("Exiting...");
            System.exit(1);
        }
    }

    public static void main(String[] args) throws Exception {
        StorageNode s = new StorageNode();
        s.createBootstrap();
        s.controllerChannel = s.connectChannel(s.contrAddress,s.contrPort);
        s.sendHeartBeatToServer();
        //s.purgeDirectory(new File(s.rootDirPath));

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            s.logger.info("Shutting down the storage node executor service");
            s.sendHeartBeat(false);
            s.logger.info("Notifying controller...");
            //shutdown executor service here
            s.executorService.shutdownNow();
            s.logger.info("storage node down!!");
        }));
    }

    private void purgeDirectory(File dir) {
        for (File file: dir.listFiles()) {
            if (file.isDirectory())
                purgeDirectory(file);
            file.delete();
        }
    }



    public void start(int port)
            throws IOException {
//    TODO    move to main
        messageRouter = new ServerMessageRouter(this);
        messageRouter.listen(port);
        this.rootDirPath += "SN"+port % 10 + "/";
        this.rootDir = rootDirPath;
        purgeDirectory(new File(this.rootDirPath));
        logger.info("this.rootDirPath : "+this.rootDirPath);
        logger.info("Listening on port : "+port +"...");
    }

    private class Worker implements Runnable{
        private int size;
        private String fileName;
        private int requestType;

        public Worker(int size, String fileName, int requestType) {
            this.size = size;
            this.fileName = fileName;
            this.requestType = requestType;
        }
        @Override
        public void run() {
            System.out.println("Threads running : ");
        }
    }

    private void sendHeartBeatToServer() {
        Runnable sendServerBeat = () -> {
            while (true){
                this.sendHeartBeat(true);
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    logger.error("Error sending heartbeat to the controller");
                    logger.error("Please check the controller");
                }
            }
        };
        Thread serverBeatThread = new Thread(sendServerBeat);
        serverBeatThread.start();
    }


    public void createBootstrap() {
        EventLoopGroup workerGroup = new NioEventLoopGroup();
        MessagePipeline pipeline = new MessagePipeline(this);

        bootstrap = new Bootstrap()
                .group(workerGroup)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.SO_KEEPALIVE, true)
                .handler(pipeline);

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
    protected void channelRead0(ChannelHandlerContext ctx, ChatMessages.ChatMessagesWrapper msg) throws Exception {
        if(msg.hasHBeat()) {

            if(msg.getHBeat().getNodeNumber() != 0) {
                // listening on port is needed only on the first beat
                this.listeningPort = 7770 + msg.getHBeat().getNodeNumber();
                this.start(this.listeningPort);
            }
            else{
                List<String> listReplicas = msg.getHBeat().getReplicaInfoList();
                this.replicasList.addAll(listReplicas);
            }
        }
        else if(msg.hasFileBuf()){
            if(msg.getFileBuf().getSource()) {
                String fileName = msg.getFileBuf().getFileName();
                byte[] buffer = msg.getFileBuf().getFileBuffer().toByteArray();
                Runnable r = new WriteFileToStorage(ctx, msg, true, this);
                executorService.execute(r);
                for(String rep : this.replicasList){
                    // send Chunk To replica SNs below function
                    String[] reg = rep.split(":");
                    Runnable r2 = new SendChunkToSN(reg[0], Integer.parseInt(reg[1]), msg, this);
                    executorService.execute(r2);
                }
            }
            else{
                // if chunk coming from SN
                //rootDir updated inside function
                // write to Storage (replicated info)
                Runnable r = new WriteFileToStorage(ctx, msg, false, this);
                executorService.execute(r);
            }
        }
        else if(msg.hasDir()){
            Runnable r4 = new ReadFileFromSNode(ctx, msg, this);
            executorService.execute(r4);
        }

        /* This method has some issues. First of all, the giant if-else block is
         * gross. More importantly, we're doing read() operations on the
         * channelRead thread, which can really be bad for performance. Ideally
         * This would be refactored as separate methods/threads and would use an
         * executor. */

        else if (msg.hasReaddirReq()) {

            Runnable r = new ProcessReadDirRequest(ctx, msg);
            executorService.execute(r);

        }
        else if (msg.hasGetattrReq()) {

            Runnable r = new ProcessGetAttrReq(ctx, msg);
            executorService.execute(r);

        }
        else if (msg.hasOpenReq()) {

            Runnable r = new ProcessHasOpenReq(ctx, msg);
            executorService.execute(r);

        }
        else if (msg.hasReadReq()) {
            Runnable r = new ProcessHasReadReq(ctx, msg);
            executorService.execute(r);
        }
//        else if(msg.hasElecBeat()){
//            manageElection(ctx, msg);
//            Runnable r2 = new Election(ctx, msg, this);
//            executorService.execute(r2);
//        }
    }

    class ProcessHasReadReq implements Runnable {
        private ChannelHandlerContext ctx;
        private ChatMessages.ChatMessagesWrapper msg;

        ProcessHasReadReq(ChannelHandlerContext ctx, ChatMessages.ChatMessagesWrapper msg){
            this.ctx = ctx;
            this.msg = msg;
        }
        public void run() {
            ChatMessages.ReadRequest read = msg.getReadReq();
            File reqPath = new File(rootDir + read.getPath());

            logger.info("read: {0}", reqPath);

            ChatMessages.ReadResponse.Builder respBuilder
                    = ChatMessages.ReadResponse.newBuilder();

            try (FileInputStream fin = new FileInputStream(reqPath)) {
                fin.skip(read.getOffset());
                byte[] contents = new byte[(int) read.getSize()];
                int readSize = fin.read(contents);
                respBuilder.setContents(ByteString.copyFrom(contents));
                respBuilder.setStatus(readSize);
            } catch (IOException e) {
                logger.warn("Error reading file", e);
                respBuilder.setStatus(0);
            }

            ChatMessages.ReadResponse resp = respBuilder.build();

            ChatMessages.ChatMessagesWrapper wrapper
                    = ChatMessages.ChatMessagesWrapper.newBuilder()
                    .setReadResp(resp)
                    .build();

            ctx.writeAndFlush(wrapper);
            logger.info("read status: {0}", resp.getStatus());
        }
    }

    class ProcessHasOpenReq implements Runnable{
        private ChannelHandlerContext ctx;
        private ChatMessages.ChatMessagesWrapper msg;
        ProcessHasOpenReq(ChannelHandlerContext ctx, ChatMessages.ChatMessagesWrapper msg){
            this.ctx = ctx;
            this.msg = msg;
        }
        public void run() {
            ChatMessages.OpenRequest open = msg.getOpenReq();
            File reqPath = new File(rootDir + open.getPath());

            logger.info("open: {0}", reqPath);

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
            logger.info("open status: {0}", resp.getStatus());
        }
    }

    class ProcessGetAttrReq implements Runnable{

        private ChannelHandlerContext ctx;
        private ChatMessages.ChatMessagesWrapper msg;

        ProcessGetAttrReq(ChannelHandlerContext ctx, ChatMessages.ChatMessagesWrapper msg){
            this.ctx = ctx;
            this.msg = msg;
        }
        public void run() {
            ChatMessages.GetattrRequest getattr = msg.getGetattrReq();
            File reqPath = new File(rootDir + getattr.getPath());

            logger.info("getattr: {0}", reqPath);

            ChatMessages.GetattrResponse.Builder respBuilder
                    = ChatMessages.GetattrResponse.newBuilder();
            respBuilder.setStatus(0);

            Set<PosixFilePermission> permissions = null;
            try {
                permissions = Files.getPosixFilePermissions(
                        reqPath.toPath(), LinkOption.NOFOLLOW_LINKS);
            } catch (IOException e) {
                logger.warn("Error reading file attributes", e);
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
            logger.info("getattr status: {0}", resp.getStatus());
        }
    }

    class ProcessReadDirRequest implements Runnable{

        private ChannelHandlerContext ctx;
        private ChatMessages.ChatMessagesWrapper msg;

        ProcessReadDirRequest(ChannelHandlerContext ctx, ChatMessages.ChatMessagesWrapper msg){
            this.ctx = ctx;
            this.msg = msg;
        }
        public void run() {
            ChatMessages.ReaddirRequest readDir = msg.getReaddirReq();
            File reqPath = new File(rootDir + readDir.getPath());

            logger.info("readdir: {0}", reqPath);

            ChatMessages.ReaddirResponse.Builder respBuilder
                    = ChatMessages.ReaddirResponse.newBuilder();
            respBuilder.setStatus(0);

            try {
                Files.newDirectoryStream(reqPath.toPath())
                        .forEach(path -> respBuilder.addContents(
                                path.getFileName().toString()));
            } catch (IOException e) {
                logger.warn("Error reading directory", e);
                respBuilder.setStatus(-ErrorCodes.ENOENT());
            }

            ChatMessages.ReaddirResponse resp = respBuilder.build();
            ChatMessages.ChatMessagesWrapper wrapper
                    = ChatMessages.ChatMessagesWrapper.newBuilder()
                    .setReaddirResp(resp)
                    .build();

            ctx.writeAndFlush(wrapper);

            logger.info("readdir status: {0}", resp.getStatus());
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        cause.printStackTrace();
    }

    class WriteFileToStorage implements Runnable{
        private ChannelHandlerContext ctx;
        private ChatMessages.ChatMessagesWrapper msg;
        private boolean source;
        private StorageNode sNode;

        WriteFileToStorage(ChannelHandlerContext ctx, ChatMessages.ChatMessagesWrapper msg, boolean source, StorageNode sNode){
            this.ctx = ctx;
            this.msg = msg;
            this.source = source;
            this.sNode = sNode;
        }
        public void run() {

            String fileName = msg.getFileBuf().getFileName();
            byte[] buffer = msg.getFileBuf().getFileBuffer().toByteArray();
            String lisPort = "";
            String sourceStr = msg.getFileBuf().getSourceHost();
            String rootPath = sNode.rootDirPath;

            // only for chunk 1 - whether from SN or client
            if (fileName.charAt(fileName.length() - 1) == '1' && fileName.charAt(fileName.length() - 2) == '_') {
                if(!source){
                    String[] reg = msg.getFileBuf().getSourceHost().split("_", 2);
                    lisPort = reg[0];
                    sourceStr = reg[1];
                }
                chunkLocations.put(fileName, sourceStr);
            }

            File file;
            String sStr = msg.getFileBuf().getSourceHost();
            if (!source) { // For replica, creating nested folder under /SN/
                if(fileName.charAt(fileName.length() - 1) == '1' && fileName.charAt(fileName.length() - 2) == '_'){
                    sStr = lisPort;
                }
                rootPath = rootPath + "/" + sStr;
                File file1 = new File(rootPath); // this file1 is for making a directory for replica
                file = new File(rootPath, fileName);
                file1.mkdirs();
            }
            else { // file coming from Client
                file = new File(rootPath, fileName);
            }

            try {
                long checksum = computeCheckSum(buffer, msg.getFileBuf().getFsize());
                checksumMap.put(fileName, checksum);
                logger.info(checksumMap.toString());
            } catch (Exception e) {
                logger.error(e.getMessage());
            }
            try (FileOutputStream fs = new FileOutputStream(file)) {
                fs.write(buffer,0 ,msg.getFileBuf().getFsize()); //TODO append fileSize > original size
            } catch (Exception e) {
                logger.error(e.getMessage());
            }
        }
    }

    private long computeCheckSum(byte[] bytes, int size) throws Exception  {
        // Ref: https://www.java-examples.com/generate-adler32-checksum-byte-array-example

        Checksum checksum = new Adler32();
        checksum.update(bytes,0, size);
        return checksum.getValue();
    }

    class ReadFileFromSNode implements Runnable{
        private ChannelHandlerContext ctx;
        private ChatMessages.ChatMessagesWrapper msg;
        private StorageNode sNode;


        public ReadFileFromSNode(ChannelHandlerContext ctx, ChatMessages.ChatMessagesWrapper msg, StorageNode sNode) {
            this.ctx = ctx;
            this.msg = msg;
            this.sNode = sNode;
        }

        @Override
        public void run(){

            String path = msg.getDir().getName();
            String parentPath = sNode.rootDirPath;
            if(path.contains("/")){
                String[] reg = path.split("/");
                logger.info("Replica folder name " + path + "   " + reg[0]);
                parentPath = parentPath + reg[0];
                path = reg[1];
            }

            byte[] buffer = new byte[sNode.chunkSize];
            File file = new File(parentPath, path);
            logger.info(file.getAbsolutePath() + " requested by client");
            try(FileInputStream fs = new FileInputStream(file);
                BufferedInputStream bs = new BufferedInputStream(fs)){
                long fileSize = file.length();
//                int chunkSize = bs.read(buffer, 0, (int)fileSize);
                int chunkSize = bs.read(buffer);
                 if(chunkSize > 0){
                    long checkSum = computeCheckSum(buffer, chunkSize);
                    logger.info("Check sum  " +  checksumMap.get(file.getName()) + " | "+ checkSum);
                    if(checkSum == checksumMap.get(file.getName())) {
                        sendChunkForReading(ctx.channel(), buffer, file.getName(), chunkSize);
                    }
                    else
                        sendErrorReplicaGraph(ctx.channel(), file.getName(),chunkSize);
                }
                else
                    logger.error("Unable to send back chunk for :" + path);
            }
            catch (Exception e){
                logger.error(String.valueOf(e));
            }
        }

        private void sendErrorReplicaGraph(Channel channel, String filename, int chunkSize){
            ChatMessages.FileInfo info = ChatMessages.FileInfo.newBuilder()
                    .setFileName(filename)
                    .setSize(chunkSize)
                    .build();
            ChatMessages.StorageNodeMetaData meta = ChatMessages.StorageNodeMetaData.newBuilder()
                    .addAllHostPort(sNode.replicasList)
                    .setFile(info)
                    .setRequestType(ChatMessages.StorageNodeMetaData.RequestType.REPLICA)
                    .build();
            ChatMessages.ChatMessagesWrapper msgWrapper = ChatMessages.ChatMessagesWrapper.newBuilder()
                    .setSMeta(meta)
                    .build();

            ChannelFuture write = channel.writeAndFlush(msgWrapper);
            write.syncUninterruptibly();
        }

    }

    class SendChunkToSN implements Runnable{

        private String hostName;
        private int port;
        private ChatMessages.ChatMessagesWrapper msg;
        private StorageNode sNode;
        SendChunkToSN(String hostName, int port, ChatMessages.ChatMessagesWrapper msg, StorageNode sNode) throws InterruptedException {
            this.hostName = hostName;
            this.port = port;
            this.msg = msg;
            this.sNode = sNode;
        }
        public void run() {

            String fileName = msg.getFileBuf().getFileName();
            byte[] buffer = msg.getFileBuf().getFileBuffer().toByteArray();
            logger.info("Replica file : "+ fileName + " | size : "+ buffer.length);
            Channel channel = null;
            try {
                channel = connectChannel(hostName, port);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            ChatMessages.FileData.Builder fileData = ChatMessages.FileData.newBuilder()
                    .setFileName(fileName)
                    .setFileBuffer(ByteString.copyFrom(buffer))
                    .setFsize(buffer.length)
                    .setSource(false)
                    .setSourceHost(sNode.listeningPort + "");

            if (fileName.charAt(fileName.length() - 1) == '1' && fileName.charAt(fileName.length() - 2) == '_') {
                // First chunk
                fileData.setSourceHost(sNode.listeningPort + "_" + msg.getFileBuf().getSourceHost());
            }

            ChatMessages.ChatMessagesWrapper msgWrapper = ChatMessages.ChatMessagesWrapper.newBuilder()
                    .setFileBuf(fileData.build())
                    .build();

            ChannelFuture write = channel.writeAndFlush(msgWrapper);
            write.syncUninterruptibly();
        }
    }

    private void sendChunkForReading(Channel channel, byte[] buffer, String chunkName, int chunkSize){
        ChatMessages.FileData.Builder fileData = ChatMessages.FileData.newBuilder()
                .setFileName(chunkName)
                .setFsize(chunkSize)
                .setFileBuffer(ByteString.copyFrom(buffer));
        int length = chunkName.length();
        if(chunkName.charAt(length-1)=='1' && chunkName.charAt(length -2) == '_')
            fileData.setSourceHost(chunkLocations.get(chunkName));

        ChatMessages.ChatMessagesWrapper msgWrapper = ChatMessages.ChatMessagesWrapper.newBuilder()
                .setFileBuf(fileData.build())
                .build();

        ChannelFuture write = channel.writeAndFlush(msgWrapper);
        write.syncUninterruptibly();
        logger.info(chunkName + " sent to client");
    }

    class ConnectChannel implements Runnable{
        private String hostName;
        private int port;
        private ChannelFuture cf;

        public ConnectChannel(String hostName, int port) {
            this.hostName = hostName;
            this.port = port;
        }

        @Override
        public void run(){
            System.out.println("Connecting to " + hostName + ":" + port);
            cf = bootstrap.connect(hostName, port);
            cf.syncUninterruptibly();
        }

        public ChannelFuture getCf() {
            return cf;
        }
    }

    private Channel connectChannel(String hostname, int port) throws InterruptedException {

        ConnectChannel connectChannel = new ConnectChannel(hostname, port);
        Thread thread = new Thread(connectChannel);
        thread.start();
        thread.join();
        ChannelFuture cf = connectChannel.getCf();
        logger.info("Connected to hostname "+hostname+":"+port);
        return cf.channel();
    }

    private void sendHeartBeat(boolean regular){
//        logger.info("Sending heartbeat...");
        int port = this.listeningPort;
        if(!regular)
            port = -2;

        ChatMessages.Heartbeat heartbeat = ChatMessages.Heartbeat.newBuilder()
                .setRequestsProcessed(this.requestsProcessed)
                .setSpaceAvailable(this.spaceAvailable)
                .setEpochTime(System.currentTimeMillis())
                .setNodeNumber(port)
                .build();


        ChatMessages.ChatMessagesWrapper msgWrapper = ChatMessages.ChatMessagesWrapper.newBuilder()
                .setHBeat(heartbeat)
                .build();
        ChannelFuture write = controllerChannel.writeAndFlush(msgWrapper);
        write.syncUninterruptibly();
//        logger.info("Heartbeat sent !");
    }

//    class Election implements Runnable{
//        private ChannelHandlerContext ctx;
//        private ChatMessages.ChatMessagesWrapper msg;
//        private StorageNode sNode;
//        public Election(ChannelHandlerContext ctx, ChatMessages.ChatMessagesWrapper msg, StorageNode sNode) {
//            this.ctx = ctx;
//            this.msg = msg;
//            this.sNode = sNode;
//        }
//        @Override
//        public void run(){
//            ChatMessages.NodeElectionBeat.BeatType beatType = msg.getElecBeat().getType();
//            if(beatType == ChatMessages.NodeElectionBeat.BeatType.LEADER_REQUEST) {
//                //This storage node was sent a message from another node that it wants to become a leader
//                checkElectState(msg.getElecBeat(), ctx);
//            }
//            else if(beatType == ChatMessages.NodeElectionBeat.BeatType.LEADER_APPROVED) {
//                // Other node approved, this node as leader
//                //this.state = ChatMessages.NodeState.LEADER;
//                sNode.voteCount++;
//                if(voteCount > replicas.length / 2){
//                    try {
//                        sendLeadershipStatus();
//                    } catch (Exception e) {
//                        e.printStackTrace();
//                    }
//                }
//            }
//            else if(beatType == ChatMessages.NodeElectionBeat.BeatType.LEADER_SELF)    {
//                //This node receives that the other node (SELF for that node) has become leader
//                sNode.term = msg.getElecBeat().getTerm();
//                sNode.state = ChatMessages.NodeState.FOLLOWER;
//                sNode.voteCount = 0;
//
//                InetSocketAddress addr = (InetSocketAddress) ctx.channel().remoteAddress();
//
//                for(int x = 0;x < replicas.length;x++){
//                    if(replicas[x].getContrAddress().equals(addr.getHostName())
//                            && replicas[x].getContrPort() == addr.getPort()){
//                        replicas[x].state = ChatMessages.NodeState.LEADER;
//                    }
//                    else {
//                        replicas[x].state = ChatMessages.NodeState.FOLLOWER;
//                    }
//                    replicas[x].setVoteCount(0);
//                }
//            }
//            else if(beatType == ChatMessages.NodeElectionBeat.BeatType.REQUEST_REJECTED)    {
//                // Request was rejected because this storage node's term is lesser
//                sNode.term = msg.getElecBeat().getTerm();
//                sNode.state = ChatMessages.NodeState.FOLLOWER;
//                sNode.voteCount = 0;
//            }
//            else if(beatType == ChatMessages.NodeElectionBeat.BeatType.LEADER_REJECTED){
//                // The other node was also a candidate, so it didn't vote for this storage node
//
//            }
//        }
//    }
//
//    private void electionTimeOut(){
//        Random r = new Random();
//        int timeOut = r.nextInt(350 - 150) + 150;
//        Timer timer = new Timer();
//        timer.schedule(new TimerTask() {
//            @Override
//            public void run() {
//                try {
//                    sendElectionRequest();
//                } catch (InterruptedException e) {
//                    e.printStackTrace();
//                }
//                // if result is not correct, call recursively
//            }
//        }, timeOut);
//    }
//
//    private void sendElectionRequest() throws InterruptedException {
//        this.voteCount = 1;
//        ChatMessages.NodeElectionBeat nodeElectionBeat = ChatMessages.NodeElectionBeat.newBuilder()
//                .setNodeState(this.state.getNumber())
//                .setVote(1)
//                .setTerm(this.term)
//                .setType(ChatMessages.NodeElectionBeat.BeatType.LEADER_REQUEST)
//                .build();
//        ChatMessages.ChatMessagesWrapper msgWrapper = ChatMessages.ChatMessagesWrapper.newBuilder()
//                .setElecBeat(nodeElectionBeat)
//                .build();
//        for(int x = 0;x < replicas.length;x++) {
//            Channel replicaChannel = connectChannel(replicas[x].getContrAddress(),replicas[x].getContrPort());
//            ChannelFuture write = replicaChannel.writeAndFlush(msgWrapper);
//            write.syncUninterruptibly();
//        }
//    }
//
//    private void writeFile(String filName, String path){
//        // If the node is leader, it will send chunk to follower
//        // with Fuse
//    }
//
//    private void retrieveFile(String fileName, String path){
//        //If node is leader, it will read the file
//        // with Fuse
//    }
//
//    private void manageElection(ChannelHandlerContext ctx, ChatMessages.ChatMessagesWrapper msg) throws Exception{
//        ChatMessages.NodeElectionBeat.BeatType beatType = msg.getElecBeat().getType();
//        if(beatType == ChatMessages.NodeElectionBeat.BeatType.LEADER_REQUEST) {
//            //This storage node was sent a message from another node that it wants to become a leader
//            checkElectState(msg.getElecBeat(), ctx);
//        }
//        else if(beatType == ChatMessages.NodeElectionBeat.BeatType.LEADER_APPROVED) {
//            // Other node approved, this node as leader
//            //this.state = ChatMessages.NodeState.LEADER;
//            this.voteCount++;
//            if(voteCount > replicas.length / 2){
//                sendLeadershipStatus();
//            }
//        }
//        else if(beatType == ChatMessages.NodeElectionBeat.BeatType.LEADER_SELF)    {
//            //This node receives that the other node (SELF for that node) has become leader
//            this.term = msg.getElecBeat().getTerm();
//            this.state = ChatMessages.NodeState.FOLLOWER;
//            this.voteCount = 0;
//
//            InetSocketAddress addr = (InetSocketAddress) ctx.channel().remoteAddress();
//
//            for(int x = 0;x < replicas.length;x++){
//                if(replicas[x].getContrAddress().equals(addr.getHostName())
//                        && replicas[x].getContrPort() == addr.getPort()){
//                    replicas[x].state = ChatMessages.NodeState.LEADER;
//                }
//                else {
//                    replicas[x].state = ChatMessages.NodeState.FOLLOWER;
//                }
//                replicas[x].setVoteCount(0);
//            }
//        }
//        else if(beatType == ChatMessages.NodeElectionBeat.BeatType.REQUEST_REJECTED)    {
//            // Request was rejected because this storage node's term is lesser
//            this.term = msg.getElecBeat().getTerm();
//            this.state = ChatMessages.NodeState.FOLLOWER;
//            this.voteCount = 0;
//        }
//        else if(beatType == ChatMessages.NodeElectionBeat.BeatType.LEADER_REJECTED){
//            // The other node was also a candidate, so it didn't vote for this storage node
//
//        }
//    }
//
//    private void sendLeadershipStatus() throws Exception {
//        ChatMessages.NodeElectionBeat leadershipBeat = ChatMessages.NodeElectionBeat.newBuilder()
//                .setNodeState(this.state.getNumber())
//                .setVote(1)
//                .setTerm(this.term)
//                .setType(ChatMessages.NodeElectionBeat.BeatType.LEADER_SELF)
//                .build();
//        ChatMessages.ChatMessagesWrapper msgWrapper = ChatMessages.ChatMessagesWrapper.newBuilder()
//                .setElecBeat(leadershipBeat)
//                .build();
//        for(int x = 0;x < replicas.length;x++) {
//            Channel replicaChannel = connectChannel(replicas[x].getContrAddress(),replicas[x].getContrPort());
//            ChannelFuture write = replicaChannel.writeAndFlush(msgWrapper);
//            write.syncUninterruptibly();
//        }
//    }
//
//    private void sendRejectionMessage(ChatMessages.NodeElectionBeat nBeat, ChannelHandlerContext ctx){
//        ChatMessages.NodeElectionBeat respBeat = ChatMessages.NodeElectionBeat.newBuilder()
//                .setType(ChatMessages.NodeElectionBeat.BeatType.REQUEST_REJECTED)
//                .setTerm(this.term)
//                .build();
//        ChatMessages.ChatMessagesWrapper msgWrapper = ChatMessages.ChatMessagesWrapper.newBuilder()
//                .setElecBeat(respBeat)
//                .build();
//
//        ChannelFuture write = ctx.writeAndFlush(msgWrapper);
//        write.syncUninterruptibly();
//    }
//
//    private void checkElectState(ChatMessages.NodeElectionBeat nBeat, ChannelHandlerContext ctx){
//        int vote = nBeat.getVote();
//        ChatMessages.NodeElectionBeat.BeatType beatType = ChatMessages.NodeElectionBeat.BeatType.LEADER_REJECTED;
//        if(nBeat.getTerm() < this.term){
//            sendRejectionMessage(nBeat,ctx);
//            return;
//        }
//        else {
//            this.term = nBeat.getTerm();
//        }
//        if(this.state != ChatMessages.NodeState.CANDIDATE){
//            vote++;
//            beatType = ChatMessages.NodeElectionBeat.BeatType.LEADER_APPROVED;
//            // I am follower
//        }
//        ChatMessages.NodeElectionBeat nodeElectionBeat = ChatMessages.NodeElectionBeat.newBuilder()
//                .setNodeState(this.state.getNumber())
//                .setVote(vote)
//                .setType(beatType)
//                .setTerm(nBeat.getTerm())
//                .build();
//
//        ChatMessages.ChatMessagesWrapper msgWrapper = ChatMessages.ChatMessagesWrapper.newBuilder()
//                .setElecBeat(nodeElectionBeat)
//                .build();
//        ChannelFuture write = ctx.writeAndFlush(msgWrapper);
//        write.syncUninterruptibly();
//
//        //check my state
//        // if candidate don't increase vote / increase term
//        //if follower increase vote
//    }

}
