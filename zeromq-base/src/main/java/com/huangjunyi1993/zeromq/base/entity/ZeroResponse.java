package com.huangjunyi1993.zeromq.base.entity;

import java.util.List;

/**
 * 服务端响应
 * Created by huangjunyi on 2022/8/21.
 */
public class ZeroResponse<T> implements Response<T> {

    private static final long serialVersionUID = 5350488331393875683L;

    // 是否成功
    private boolean success;

    // 是否为ACK
    private boolean isAck;

    // 错误码
    private int errorCode;

    // 错误信息
    private String errorMessage;

    // 响应id，与客户端发送过来的id一致
    private long id;

    // 消费偏移量
    private long offset;

    // 响应体（如果是对消息请求的响应，就是请求的消息）
    private List<T> data;

    public ZeroResponse(boolean success, int errorCode, String errorMessage, long id, long offset, List<T> data, boolean isAck) {
        this.success = success;
        this.errorCode = errorCode;
        this.errorMessage = errorMessage;
        this.id = id;
        this.offset = offset;
        this.data = data;
        this.isAck = isAck;
    }

    public static <T> ZeroResponse success(long id, long offset, List<T> data, boolean isAck) {
        return new ZeroResponse(true, 0, null, id, offset, data, isAck);
    }

    public static ZeroResponse success(long id,  boolean isAck) {
        return new ZeroResponse(true, 0, null, id, -1, null, isAck);
    }

    public static ZeroResponse failed(long id, long offset, int errorCode, String errorMessage, boolean isAck) {
        return new ZeroResponse(false, errorCode, errorMessage, id, offset, null, isAck);
    }

    @Override
    public boolean isSuccess() {
        return this.success;
    }

    @Override
    public boolean isAck() {
        return this.isAck;
    }

    @Override
    public int errorCode() {
        return this.errorCode;
    }

    @Override
    public String errorMessage() {
        return this.errorMessage;
    }

    @Override
    public long getId() {
        return this.id;
    }

    @Override
    public long getOffset() {
        return this.offset;
    }

    @Override
    public List<T> getData() {
        return this.data;
    }

    @Override
    public String toString() {
        return "ZeroResponse{" +
                "success=" + success +
                ", isAck=" + isAck +
                ", errorCode=" + errorCode +
                ", errorMessage='" + errorMessage + '\'' +
                ", id=" + id +
                ", offset=" + offset +
                ", data=" + data +
                '}';
    }
}
