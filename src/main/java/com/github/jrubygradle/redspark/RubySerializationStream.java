package com.github.jrubygradle.redspark;

import org.apache.hadoop.io.serializer.JavaSerialization;
import org.apache.spark.serializer.JavaSerializationStream;
import org.apache.spark.serializer.SerializationStream;
import scala.reflect.ClassTag;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.util.logging.Logger;

public class RubySerializationStream extends JavaSerializationStream {
    private static final Logger log = Logger.getLogger( RubySerializationStream.class.getName() );

    public RubySerializationStream(OutputStream out) {
        super(out, 1000, true);
    }
    public <T> SerializationStream writeObject(T t, ClassTag<T> evidence) {
        log.info("serializing writeObject");
        log.info(t.getClass().toString());
        return super.writeObject(t, evidence);
    }
}
