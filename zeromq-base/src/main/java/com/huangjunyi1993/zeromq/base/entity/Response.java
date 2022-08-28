package com.huangjunyi1993.zeromq.base.entity;

import java.io.Serializable;
import java.util.List;

/**
 * 服务端消息接口
 * Created by huangjunyi on 2022/8/11.
 */
public interface Response<T> extends Serializable {

    boolean isSuccess();

    boolean isAck();

    int errorCode();

    String errorMessage();

    long getId();

    long getOffset();

    List<T> getData();

}
