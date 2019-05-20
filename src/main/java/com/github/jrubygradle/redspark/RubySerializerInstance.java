package com.github.jrubygradle.redspark;

import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.logging.Logger;

import org.apache.spark.serializer.JavaSerializerInstance;
import scala.reflect.ClassTag;

import org.apache.spark.serializer.SerializerInstance;
import org.apache.spark.serializer.DeserializationStream;
import org.apache.spark.serializer.SerializationStream;

public class RubySerializerInstance<T> extends JavaSerializerInstance {
    private static final Logger log = Logger.getLogger( RubySerializerInstance.class.getName() );

    public RubySerializerInstance(int counterReset, boolean extraDebugInfo, ClassLoader loader) {
        super(counterReset, true, loader);
    }

    public <T> T deserialize(ByteBuffer bytes, ClassLoader loader, ClassTag<T> evidence) {
        log.info("deserialize with loader");
        return super.deserialize(bytes, loader, evidence);
    }

    public <T> T deserialize(ByteBuffer bytes, ClassTag<T> evidence) {
        log.info("deserialize");
        return super.deserialize(bytes, evidence);
    }

    public <T> ByteBuffer serialize(T t,  ClassTag<T> evidence) {
        log.info("serialize");
        return super.serialize(t, evidence);
    }

    public DeserializationStream deserializeStream(InputStream stream) {
        log.info("deserialize stream");
        return super.deserializeStream(stream);
    }

    public SerializationStream serializeStream(OutputStream stream) {
        return new RubySerializationStream(stream);
    }
}
