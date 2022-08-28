package com.huangjunyi1993.zeromq.base.entity;

import java.util.List;

/**
 * 服务端响应
 * Created by huangjunyi on 2022/8/21.
 */
public class ZeroResponse<T> implements Response<T> {

    private static final long serialVersionUID = 5350488331393875683L;
    private boolean success;
    private boolean isAck;
    private int errorCode;
    private String errorMessage;
    private long id;
    private long offset;
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
