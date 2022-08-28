package com.huangjunyi1993.zeromq.base.serializer;

import com.huangjunyi1993.zeromq.base.exception.SerializerException;

import java.io.*;

import static com.huangjunyi1993.zeromq.base.enums.SerializationTypeEnum.JDK_NATIVE_SERIALIZATION;

/**
 * JDK序列化器
 * Created by huangjunyi on 2022/8/13.
 */
public class JDKNativeSerializer implements Serializer {
    @Override
    public <T> T deserialize(byte[] bytes) throws ClassNotFoundException, IOException {
        if (bytes == null || bytes.length == 0) {
            throw new SerializerException("The input binary data is empty");
        }

        ByteArrayInputStream bais;
        ObjectInputStream ois = null;
        try {
            bais = new ByteArrayInputStream(bytes);
            ois = new ObjectInputStream(bais);
            return (T) ois.readObject();
        } catch (IOException e) {
            throw new SerializerException("Deserialization failed", e);
        } finally {
            if (ois != null) {
                ois.close();
            }
        }
    }

    @Override
    public <T> byte[] serialize(T t) throws IOException {
        if (!(t instanceof Serializable)) {
            throw new SerializerException("Objects that do not implement a serialization interface cannot be serialized");
        }

        ByteArrayOutputStream baos;
        ObjectOutputStream oos = null;
        try {
            baos = new ByteArrayOutputStream();
            oos = new ObjectOutputStream(baos);
            oos.writeObject(t);
            oos.flush();
            return  baos.toByteArray();
        } catch (IOException e) {
            throw new SerializerException("Serialization failed", e);
        } finally {
            if (oos != null) {
                oos.close();
            }
        }
    }

    @Override
    public int getType() {
        return JDK_NATIVE_SERIALIZATION.getType();
    }
}
