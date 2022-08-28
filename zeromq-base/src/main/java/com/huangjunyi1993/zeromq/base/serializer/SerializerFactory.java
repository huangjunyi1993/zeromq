package com.huangjunyi1993.zeromq.base.serializer;

import com.huangjunyi1993.zeromq.base.exception.SerializerException;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 序列化器工厂
 * Created by huangjunyi on 2022/8/13.
 */
public class SerializerFactory {

    private SerializerFactory() {
    }

    private static Map<Integer, Serializer> SERIALIZER_INSTANCE_CACHE = new ConcurrentHashMap<>();

    static {
        ServiceLoader<Serializer> serializerServiceLoader = ServiceLoader.load(Serializer.class);
        for (Serializer serializer : serializerServiceLoader) {
            register(serializer);
        }
    }

    public static Serializer getSerializer(int type) {
        if (SERIALIZER_INSTANCE_CACHE.containsKey(type)) {
            return SERIALIZER_INSTANCE_CACHE.get(type);
        }
        throw new SerializerException(String.format("No serializer of type %s could be found", type));
    }

    public static void register(Serializer serializer) {
        int type = serializer.getType();
        if (SERIALIZER_INSTANCE_CACHE.containsKey(type)) {
            throw new SerializerException(String.format("A serializer type of %s already exists", type));
        }
        SERIALIZER_INSTANCE_CACHE.put(type, serializer);
    }

}
