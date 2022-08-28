package com.huangjunyi1993.zeromq.base.serializer;

import com.caucho.hessian.io.Hessian2Input;
import com.caucho.hessian.io.Hessian2Output;
import com.huangjunyi1993.zeromq.base.exception.SerializerException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;

import static com.huangjunyi1993.zeromq.base.enums.SerializationTypeEnum.HESSIAN2_SERIALIZATION;

/**
 * Hessian2序列化器
 * Created by huangjunyi on 2022/8/13.
 */
public class Hessian2Serializer implements Serializer {
    @Override
    public <T> T deserialize(byte[] bytes) throws IOException {

        ByteArrayInputStream bais = null;
        Hessian2Input hi = null;

        try {
            bais = new ByteArrayInputStream(bytes);
            hi = new Hessian2Input(bais);
            return (T) hi.readObject();
        } catch (IOException e) {
            throw new SerializerException("Deserialization failed", e);
        } finally {
            if (hi != null) {
                hi.close();
            }
        }
    }

    @Override
    public <T> byte[] serialize(T t) throws IOException {
        if (!(t instanceof Serializable)) {
            throw new SerializerException("Objects that do not implement a serialization interface cannot be serialized");
        }

        ByteArrayOutputStream baos;
        Hessian2Output ho = null;

        try {
            baos = new ByteArrayOutputStream();
            ho = new Hessian2Output(baos);
            ho.writeObject(t);
            ho.flush();
            return baos.toByteArray();
        } catch (IOException e) {
            throw new SerializerException("Serialization failed", e);
        } finally {
            if (ho != null) {
                ho.close();
            }
        }
    }

    @Override
    public int getType() {
        return HESSIAN2_SERIALIZATION.getType();
    }
}
