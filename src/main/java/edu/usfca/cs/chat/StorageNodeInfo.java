package edu.usfca.cs.chat;

import edu.usfca.cs.chat.util.ReadConfig;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;

import java.io.IOException;
import java.util.HashSet;

public class StorageNodeInfo {
    private double spaceAvailable;
    private long lastBeatTime;
    private int listeningPort;
    private String hostName;
    private HashSet<StorageNodeInfo> replicaSet;

    public void setBloomFilter(BloomFilter bloomFilter) {
        this.bloomFilter = bloomFilter;
    }

    private BloomFilter bloomFilter;
    private int controllerPort;

    protected StorageNodeInfo(double spaceAvailable, long lastBeatTime) throws IOException {
        ReadConfig config = new ReadConfig();
        this.spaceAvailable = spaceAvailable;
        this.lastBeatTime = lastBeatTime;
        this.replicaSet = new HashSet<>();
        this.bloomFilter = new BloomFilter(config.getBloomFilterSize());
    }

    public BloomFilter getBloomFilter() {
            return bloomFilter;
    }

    public int getControllerPort() {
        return controllerPort;
    }

    public void setControllerPort(int controllerPort) {
        this.controllerPort = controllerPort;
    }

    @Override
    public String toString() {
        return "StorageNodeInfo{" +
                "bloomFilter=" + bloomFilter +
                '}';
    }

    public void putFileInBloomFilter(String fileName){
            this.getBloomFilter().put(fileName);
    }

    public boolean checkFileInBloomFilter(String fileName){
        return this.getBloomFilter().get(fileName);
    }

    public String getHostName() {
        return hostName;
    }

    public void setHostName(String hostName) {
        this.hostName = hostName;
    }

    public HashSet<StorageNodeInfo> getReplicaSet() {
        return replicaSet;
    }

    public void setReplicaSet(HashSet<StorageNodeInfo> replicaSet) {
        this.replicaSet = replicaSet;
    }

    public int getListeningPort() {
        return listeningPort;
    }

    public void setListeningPort(int listeningPort) {
        this.listeningPort = listeningPort;
    }

    protected double getSpaceAvailable() {
        return spaceAvailable;
    }

    protected void setSpaceAvailable(double spaceAvailable) {
        this.spaceAvailable = spaceAvailable;
    }

    protected long getLastBeatTime() {
        return lastBeatTime;
    }

    protected void setLastBeatTime(long lastBeatTime) {
        this.lastBeatTime = lastBeatTime;
    }

//    private void checkInBloomFilter(ChannelHandlerContext ctx, ChatMessages.ChatMessagesWrapper msg){
//        double size = msg.getFInfo().getSize();
//        String filename = msg.getFInfo().getFileName();
//        if(size != -1){
//            if(this.getBloomFilter().get(filename)){
//                if(this.rTable.checkFileExists(filename)){
//                    String dir = rootDirPath + filename;
//                    ChatMessages.DirectoryMsg directoryMsg = ChatMessages.DirectoryMsg.newBuilder()
//                            .setName(dir)
//                            .build();
//                    ChatMessages.ChatMessagesWrapper msgWrapper = ChatMessages.ChatMessagesWrapper.newBuilder()
//                            .setDir(directoryMsg)
//                            .build();
//                    ChannelFuture write = ctx.channel().writeAndFlush(msgWrapper);
//                    write.syncUninterruptibly();
//                }
//            }
//
//        }
//    }


}
