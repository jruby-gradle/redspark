#!/usr/bin/env ruby
#
# To run, first execute `./gradlew jrubyJar` to package the jar, then call
# `./run.sh` to send the jar to a local spark cluster installation
#

java_import 'org.apache.spark.sql.SparkSession'
java_import 'org.apache.spark.api.java.function.FilterFunction'
java_import 'org.apache.spark.api.java.function.ForeachFunction'

logfile = 'build.gradle'
spark = SparkSession.builder.appName('Simple Application').getOrCreate
data = spark.read.textFile(logfile).cache()

class BeeForeach
  include org.apache.spark.api.java.function.ForeachFunction
  def call(item)
    puts "foreaching item: #{item}"
  end
end
class BeeFilter
  include org.apache.spark.api.java.function.FilterFunction
  def call(item)
    puts "filtering item: #{item}"
  end
end

alphas = data.distinct
puts "about to filter"
#
# Failure caused while deserializing on the spark worker
#
# java.lang.ClassCastException: cannot assign instance of
# scala.collection.immutable.List$SerializationProxy to field
# org.apache.spark.rdd.RDD.org$apache$spark$rdd$RDD$$dependencies_ of typ
#betas = data.filter(BeeFilter.new).count

# Failure caused while deserializting on the spark worker
#
# java.lang.ClassNotFoundException: org.jruby.gen.BeeForeach_799252494
betas = data.foreach(BeeForeach.new).count

# Failure caused while serializing on the spark master
#
# java.io.IOException: can not serialize singleton object
#betas = data.filter { |line| line.contains('b') }.count
puts "filtered"

puts
puts "Hello from Ruby, we read #{logfile}"
puts " and found #{alphas} 'a' characters"
puts " and #{betas} 'b' characters"
puts 

spark.stop()
