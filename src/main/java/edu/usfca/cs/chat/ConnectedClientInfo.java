package edu.usfca.cs.chat;

import io.netty.channel.ChannelHandlerContext;

public class ConnectedClientInfo {
    public ConnectedClientInfo(String requestType, ChannelHandlerContext ctx, String fileName) {
        this.requestType = requestType;
        this.ctx = ctx;
        this.fileName = fileName;
    }

    public String getRequestType() {
        return requestType;
    }

    public void setRequestType(String requestType) {
        this.requestType = requestType;
    }

    public ChannelHandlerContext getCtx() {
        return ctx;
    }

    public void setCtx(ChannelHandlerContext ctx) {
        this.ctx = ctx;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    private String requestType;
    private ChannelHandlerContext ctx;
    private String fileName;



}
