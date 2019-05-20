package com.github.jrubygradle.redspark;

import java.io.Serializable;
import org.apache.spark.serializer.Serializer;
import org.apache.spark.serializer.SerializerInstance;

/**
 * RubySerializer implements ruby object serializer for writing Ruby spark jobs.
 *
 * It should be configured at job submit time, e.g.
 *
 *    java_import 'org.apache.spark.sql.SparkSession'
 *    SparkSession
 *      .builder
 *      .config('spark.serializer', 'com.github.jrubygradle.redspark.RubySerializer')
 *      .master('local[*]')
 *      .getOrCreate
 *
 */
public class RubySerializer extends Serializer {
    /**
     * Return the {@code SerializerInstance} required by the {@code Serializer}
     * abstract class
     */
    public SerializerInstance newInstance() {
        return new RubySerializerInstance(1000,
                true,
                Thread.currentThread().getContextClassLoader());
    }
}
