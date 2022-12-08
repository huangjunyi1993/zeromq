package com.huangjunyi1993.zeromq.remoting;

import com.huangjunyi1993.zeromq.base.Context;
import com.huangjunyi1993.zeromq.base.ZeroContext;
import com.huangjunyi1993.zeromq.base.entity.Message;
import com.huangjunyi1993.zeromq.base.entity.Response;
import com.huangjunyi1993.zeromq.base.entity.ZeroResponse;
import com.huangjunyi1993.zeromq.base.exception.ServerHandleException;
import com.huangjunyi1993.zeromq.base.protocol.ZeroProtocol;
import com.huangjunyi1993.zeromq.base.serializer.Serializer;
import com.huangjunyi1993.zeromq.base.serializer.SerializerFactory;
import com.huangjunyi1993.zeromq.core.Handler;
import com.huangjunyi1993.zeromq.core.HandlerFactory;
import com.huangjunyi1993.zeromq.core.handler.ZeroHandlerFactory;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.List;

import static com.huangjunyi1993.zeromq.base.constants.ContextVariableConstant.*;
import static com.huangjunyi1993.zeromq.base.enums.MessageTypeEnum.*;

/**
 * 服务器处理器
 * Created by huangjunyi on 2022/8/20.
 */
public class ZeroServerHandler extends SimpleChannelInboundHandler<ZeroProtocol> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ZeroServerHandler.class);

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, ZeroProtocol protocol) throws Exception {
        // 异步处理请求
        ctx.channel().eventLoop().submit(() -> {
            boolean handleSuccess = true;
            Context context = null;
            try {
                // 处理器工厂
                HandlerFactory handlerFactory = ZeroHandlerFactory.getInstance();
                // 创建处理器,如果已经创建会重用，返回的是处理器链
                Handler handler = handlerFactory.build(protocol);
                // 上下文对象：上处理器链中传递 协议对象 变量表
                context = new ZeroContext();
                context.setProtocal(protocol);
                // 处理器链执行
                handler.handle(context);
                if (context.getVariable(CONTEXT_VARIABLE_HANDLE_SUCCESS) != null) {
                    handleSuccess = (boolean) context.getVariable(CONTEXT_VARIABLE_HANDLE_SUCCESS);
                }
            } catch (ServerHandleException e) {
                LOGGER.info("The server Handler failed to process the request:", e);
                handleSuccess = false;
            } catch (Exception e) {
                LOGGER.info("System internal error:", e);
                handleSuccess = false;
            }

            // 根据请求协议报文中的消息类型，确定响应内容
            Response<Message> response = null;
            if (protocol.getMessageType() == MESSAGE.getType()) {
                response = handleSuccess ? ZeroResponse.success(protocol.getId(), false) : ZeroResponse.failed(protocol.getId(), -1, (int) context.getVariable(CONTEXT_VARIABLE_ERROR_CODE), (String) context.getVariable(CONTEXT_VARIABLE_ERROR_MESSAGE), false);
            } else if (protocol.getMessageType() == REQUEST.getType()) {
                response = handleSuccess ? ZeroResponse.success(protocol.getId(), (Long) context.getVariable(CONTEXT_VARIABLE_BATCH_OFFSET), (List<Message>) context.getVariable(CONTEXT_VARIABLE_RESULT), false) : ZeroResponse.failed(protocol.getId(), -1, (int) context.getVariable(CONTEXT_VARIABLE_ERROR_CODE), (String) context.getVariable(CONTEXT_VARIABLE_ERROR_MESSAGE), false);;
            } else if (protocol.getMessageType() == ACK.getType()) {
                response = handleSuccess ? ZeroResponse.success(protocol.getId(), true) : ZeroResponse.failed(protocol.getId(), -1, (int) context.getVariable(CONTEXT_VARIABLE_ERROR_CODE), (String) context.getVariable(CONTEXT_VARIABLE_ERROR_MESSAGE), true);
            }

            try {
                if (response != null) {
                    // 序列化响应对象
                    Serializer serializer = SerializerFactory.getSerializer(protocol.getSerializationType());
                    byte[] bytes = serializer.serialize(response);
                    // 生成响应协议对象并写入通道
                    ctx.channel().writeAndFlush(new ZeroProtocol(bytes.length, protocol.getSerializationType(), RESPONSE.getType(), protocol.getId(), bytes));
                }
            } catch (IOException e) {
                LOGGER.info("An exception occurred while the server was sending back a response:", e);
            }
        });
    }
}
