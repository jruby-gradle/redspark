#!/usr/bin/env ruby

java_import 'org.apache.spark.sql.SparkSession'

class SimpleApp
    def main(args)
        logfile = 'build.gradle'
        spark = SparkSession.builder.appName('Simple Application').getoOrCreate()
        data = spark.read.textFile(logfile).cache()

        #val numAs = logData.filter(line => line.contains("a")).count()
        #val numBs = logData.filter(line => line.contains("b")).count()

        puts "Hello World"
        spark.stop()
    end
end
