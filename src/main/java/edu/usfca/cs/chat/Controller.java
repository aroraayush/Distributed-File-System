package edu.usfca.cs.chat;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import edu.usfca.cs.chat.net.ServerMessageRouter;
import edu.usfca.cs.chat.util.Directory;
import edu.usfca.cs.chat.util.ReadConfig;
import io.netty.channel.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ChannelHandler.Sharable
public class Controller
    extends SimpleChannelInboundHandler<ChatMessages.ChatMessagesWrapper> {
    //This is the NameNode
    // TODO: Controller removes existing files on Orion on startup

    private ExecutorService executorService;
    private static ConcurrentHashMap<String, Directory> mapDirectory;
    private Logger logger = LoggerFactory.getLogger(Controller.class.getName());
    private int nodeNumber; // A counter that will assign a number to node (it will only increment)
    private static ConcurrentHashMap<String,ChannelHandlerContext> mapNodes;
    private HashSet<ConnectedClientInfo> connectedClients;
    private static ConcurrentHashMap<String,StorageNodeInfo> nodesInfo;  // List of active storage Nodes
    private static ConcurrentHashMap<String,Integer> portMap;  // List of active storage Nodes
    private ServerMessageRouter messageRouter;
    private int replicationFactor;
    private int chunkSize;

    public Controller(int replicationFactor, int chunkSize) throws IOException {
        this.executorService = Executors.newCachedThreadPool();
        this.mapDirectory = new ConcurrentHashMap<>();
        this.mapNodes = new ConcurrentHashMap<>();
        this.connectedClients = new HashSet<>();
        this.nodesInfo = new ConcurrentHashMap<>();
        this.nodeNumber = 0;
        this.replicationFactor = replicationFactor;
        this.chunkSize = chunkSize;
        portMap = new ConcurrentHashMap<>();
    }

    public void start(int port)
    throws IOException {
        messageRouter = new ServerMessageRouter(this);
        messageRouter.listen(port);
        logger.info("Listening on port : "+port +"...");
    }

    public static void main(String[] args) throws IOException {
        ReadConfig config = new ReadConfig();
//        Controller c = new Controller(config.getReplicationFactor(), config.getsNDirPath(), config.getChunkSize());
        Controller c = new Controller(config.getReplicationFactor(), config.getChunkSize());
        try {
            c.start(config.getControllerPort());
            c.checkNodeFailure();
        } catch (IOException e) {
            c.logger.error("Either unable to read config or check for Node failure");
            c.logger.error(e.getMessage());
            c.logger.error("Exiting...");
            System.exit(1);
        }
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            c.logger.info("Shutting down the executor service");
            //shutdown executor service here
            c.executorService.shutdownNow();
        }));
    }

    private void checkNodeFailure() {
        Runnable checkNodeHealth = () -> {
            while (true){
                this.checkActiveNodes();
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    logger.error("Error checking storage node's health.");
                    logger.error("Error : "+ e.getMessage());
                    logger.error("Exiting...");
                    System.exit(1);
                }
            }
        };
        Thread failureDetectionThread = new Thread(checkNodeHealth);
        failureDetectionThread.start();
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

    @Override
    public void channelRead0(
            ChannelHandlerContext ctx, ChatMessages.ChatMessagesWrapper msg) {

        if(msg.hasHBeat()){
            processHBeat(ctx,msg);
        }
        else if(msg.hasFInfo()){
            Runnable r = new ProcessFInfo(ctx, msg);
            executorService.execute(r);

        }
        else if(msg.hasRepStatus()){
            String sourceHost = msg.getRepStatus().getSourceHost();
            String[] reg = sourceHost.split(",");
            for(int x = 0;x < reg.length;x++){

                String[] reg1 = reg[x].split(":");
                String hostPort1 = reg1[0] + ":" + portMap.get(reg[x]);
                if(!mapNodes.containsKey(hostPort1)){
                    HashSet<StorageNodeInfo> replicas = nodesInfo.get(hostPort1).getReplicaSet();
                    for(StorageNodeInfo node : replicas){
                        String hostPort = node.getHostName() + ":" + node.getControllerPort();
                        if(mapNodes.containsKey(hostPort)){
                            reg[x] = node.getHostName() + ":" + node.getListeningPort() + ":" + reg1[1];
                            break;
                        }
                    }
                }
            }

            ChatMessages.ReplicaStatus replicaStatus = ChatMessages.ReplicaStatus.newBuilder()
                    .setFilename(msg.getRepStatus().getFilename())
                    .setSourceHost(String.join(",",  reg))
                    .build();
            ChatMessages.ChatMessagesWrapper msgWrapper = ChatMessages.ChatMessagesWrapper.newBuilder()
                    .setRepStatus(replicaStatus)
                    .build();
            ChannelFuture write = ctx.channel().writeAndFlush(msgWrapper);
            write.syncUninterruptibly();
        }
    }

    class ProcessFInfo implements Runnable {

        private ChannelHandlerContext ctx;
        private ChatMessages.ChatMessagesWrapper msg;

        public ProcessFInfo(ChannelHandlerContext ctx, ChatMessages.ChatMessagesWrapper msg) {
            this.ctx = ctx;
            this.msg = msg;
        }
        @Override
        public void run() {
            double size = msg.getFInfo().getSize();
            String fileName = msg.getFInfo().getFileName();
            if (size != -1) {
                // Write request received
                sendAssignedSNToClient(size, ctx, fileName);
            } else {
                // Read request received
                connectedClients.add(new ConnectedClientInfo("READ", ctx, fileName));
                String keyPath = getChunkFileSNInfo(msg);
                ArrayList<String> list = new ArrayList<>();
                for(ConnectedClientInfo clientInfo : connectedClients){
                    if(clientInfo.getFileName().equals(msg.getFInfo().getFileName()) && clientInfo.getRequestType().equals("READ")){
                        if(keyPath != null){
                            String[] reg = keyPath.split("_",2);
                            list.add(reg[0]);
                            sendStorageMetaData(list, ctx.channel(), ChatMessages.StorageNodeMetaData.RequestType.READ, reg[1]);
                        }
                      else{
                        // File Not found
                        sendStorageMetaData(list, ctx.channel(), ChatMessages.StorageNodeMetaData.RequestType.READ, fileName);
                      }
                        clientInfo.setRequestType("REQ_PROCESSED");
                    }
                }
            }
        }
    }

    private void processHBeat(ChannelHandlerContext ctx, ChatMessages.ChatMessagesWrapper msg) {
        ChatMessages.Heartbeat.Builder beatResponse = ChatMessages.Heartbeat.newBuilder();

        ChatMessages.Heartbeat beat = msg.getHBeat();
        InetSocketAddress addr
                = (InetSocketAddress) ctx.channel().remoteAddress();
        String key = addr.getHostName() + ":" + addr.getPort();
        mapNodes.put(key,ctx);
        if(!mapDirectory.containsKey(key)) {
            mapDirectory.put(key, new Directory("", true));
        }
        StorageNodeInfo storageNodeInfo = null;
        try {
            storageNodeInfo = new StorageNodeInfo(beat.getSpaceAvailable(),
                    beat.getEpochTime() / 1000);
        } catch (IOException e) {
            e.printStackTrace();
        }
        storageNodeInfo.setHostName(addr.getHostName());

        int rep = 0;
        HashSet<StorageNodeInfo> repSet = new HashSet<>();
        HashSet<String> repProtoSet = new HashSet<>();

        if(nodesInfo.containsKey(key)){
            BloomFilter bloomFilter = nodesInfo.get(key).getBloomFilter();
            int controllerP = nodesInfo.get(key).getControllerPort();
            storageNodeInfo.setControllerPort(controllerP);
            storageNodeInfo.setBloomFilter(bloomFilter);
            rep = nodesInfo.get(key).getReplicaSet().size(); //1
            repSet = nodesInfo.get(key).getReplicaSet();
            for(StorageNodeInfo node : repSet){
                repProtoSet.add(node.getHostName() +":"+ node.getListeningPort());
            }
        }
        if(nodesInfo.size() > 1 && rep < replicationFactor){
            for(String node : nodesInfo.keySet()){ // TODO  // deletion of faulted replicas
                if(rep >= replicationFactor)
                    break;
                String str = nodesInfo.get(node).getHostName() +":"+ nodesInfo.get(node).getListeningPort();
                if(node.equals(key) || repProtoSet.contains(str))
                    continue;
                repProtoSet.add(str);
                repSet.add(nodesInfo.get(node));
                rep++;
            }
            System.out.println(key + "  =  " + repProtoSet);
            storageNodeInfo.setReplicaSet(repSet);
            beatResponse.addAllReplicaInfo(repProtoSet);
        }
        if(beat.getNodeNumber() == -1){
            this.nodeNumber = this.nodeNumber + 1;
            int listeningPort = 7770 + this.nodeNumber;
            storageNodeInfo.setListeningPort(listeningPort);
            storageNodeInfo.setControllerPort(addr.getPort());
            portMap.put(addr.getHostName()+":"+listeningPort, addr.getPort());
            beatResponse.setNodeNumber(this.nodeNumber);
        }
        else if(beat.getNodeNumber() == -2){
            // Node down
            logger.info(key +" down.");
            logger.info("Removing it from list of available nodes.");
            mapNodes.remove(key);
            return;
        }
        else {
            storageNodeInfo.setListeningPort(beat.getNodeNumber());
        }
        nodesInfo.put(key,storageNodeInfo);


        ChatMessages.ChatMessagesWrapper msgWrapper = ChatMessages.ChatMessagesWrapper.newBuilder()
                .setHBeat(beatResponse.build())
                .build();
        ChannelFuture write = ctx.channel().writeAndFlush(msgWrapper);
        write.syncUninterruptibly();
    }

    private String directoryRead(Directory rootDir, String fileName, String path){

        if(!rootDir.isDirectory() && rootDir.getName().equals(fileName))
            return path;
        else if(!rootDir.isDirectory())
            return "";
        else if(rootDir.isDirectory()){
            for(int x = 0;x < rootDir.getChildren().size();x++){
                return directoryRead(rootDir.getChildren().get(x), fileName, path +
                        rootDir.getName()+ rootDir.getChildren().get(x).getName());
            }
        }
        return path;
    }


    private String getChunkFileSNInfo(ChatMessages.ChatMessagesWrapper msg){
        //read file
        String fileName = msg.getFInfo().getFileName();
        for(String node : nodesInfo.keySet()){

            if(nodesInfo.get(node).checkFileInBloomFilter(fileName)){

                logger.info("File may be there as per bloom filter.. ");
                String path = directoryRead(mapDirectory.get(node), fileName, "");
                if(path.length() > 0){
                    if(!mapNodes.containsKey(node)){
                        if(mapNodes.size() == 0)
                            return null;
                        HashSet<StorageNodeInfo> nodeInfo = nodesInfo.get(node).getReplicaSet();
                        for(StorageNodeInfo info : nodeInfo){
                            String key = info.getHostName() + ":" + info.getControllerPort();
                            if(mapNodes.containsKey(key)){
                                return info.getHostName() + ":" + info.getListeningPort() + ":" + nodesInfo.get(node).getListeningPort() + "_" + path;
                            }
                        }
                    }
//                    return node + "_" + path;
                    return nodesInfo.get(node).getHostName() + ":" + nodesInfo.get(node).getListeningPort() + "_" + path;

                }
            }
        }
        logger.info("File Not found at any storage nodes " + fileName);
        return null;

    }

    private void sendAssignedSNToClient(double size, ChannelHandlerContext ctx, String fileName){
        //using bloom filter decide which active node to choose
        removeIfFileExists(fileName);
        String[] key = new String[1];
        ArrayList<String> optimalNodes = chooseStorageNode(size, fileName, key);

        if(optimalNodes.size() > 0){
            Directory dir = mapDirectory.get(key[0]);
            dir.addChild(fileName,false);
        }
        sendStorageMetaData(optimalNodes, ctx.channel(), ChatMessages.StorageNodeMetaData.RequestType.WRITE, fileName);

    }

    private void removeIfFileExists(String fileName) {
        for(String node : nodesInfo.keySet()){
            if(nodesInfo.get(node).checkFileInBloomFilter(fileName)){
                mapDirectory.get(node).removeChild(mapDirectory.get(node),fileName);
            }
        }
    }

    private ArrayList<String> chooseStorageNode(double fileSize, String fileName, String[] portNumber){
        ArrayList<String> hostPort = new ArrayList<>();
        int numChunks = fileSize % 10.0 == 0 ? (int)fileSize / 10 : ((int)fileSize / 10) + 1;
        int flag = 0;
        while (hostPort.size() < numChunks){

                for(String node : nodesInfo.keySet()) {
                    double cSize = fileSize > this.chunkSize / (1024 * 1024) ? this.chunkSize / (1024 * 1024) : fileSize;
                    double space = nodesInfo.get(node).getSpaceAvailable();

                    if (space >= cSize) {
                        String[] nodeStr = node.split(":");
                        hostPort.add(nodeStr[0] + ":" + nodesInfo.get(node).getListeningPort());
                        if (flag == 0) {
                            System.out.println(nodesInfo.get(node));
                            nodesInfo.get(node).putFileInBloomFilter(fileName);
                            System.out.println(nodesInfo.get(node));
                            portNumber[0] = node;
                            flag = 1;
                        }
                        nodesInfo.get(node).setSpaceAvailable(space - cSize);
                        fileSize -= cSize;
                    }
                    if (hostPort.size() == numChunks)
                        break;
                }

            }
        return hostPort;
    }

    private void sendStorageMetaData(ArrayList<String> optimalNodes, Channel channel, ChatMessages.StorageNodeMetaData.RequestType requestType, String filename){

        ChatMessages.StorageNodeMetaData.Builder sMeta = ChatMessages.StorageNodeMetaData.newBuilder()
                .addAllHostPort(optimalNodes)
                .setRequestType(requestType);

        if(requestType == ChatMessages.StorageNodeMetaData.RequestType.READ ||
                requestType == ChatMessages.StorageNodeMetaData.RequestType.WRITE){
            sMeta.setFile(ChatMessages.FileInfo.newBuilder().setFileName(filename).build());
        }

        ChatMessages.ChatMessagesWrapper msgWrapper = ChatMessages.ChatMessagesWrapper.newBuilder()
                .setSMeta(sMeta.build())
                .build();
        ChannelFuture write = channel.writeAndFlush(msgWrapper);
        write.syncUninterruptibly();
    }

    private void checkActiveNodes(){
        long currTime;
        int diff;
        for(String key : mapNodes.keySet()){
            currTime = System.currentTimeMillis();
            diff = (int) ((currTime/1000) - nodesInfo.get(key).getLastBeatTime());

            // If difference more than 5ms,
            // removing it from list of available nodes
            if(diff > 6){
                logger.info("Last heart beat from server :"+key+ " came "+ diff +" seconds ago.");
                logger.info("Removing it from list of available nodes.");
                mapNodes.remove(key);
            }
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        cause.printStackTrace();
    }
}
