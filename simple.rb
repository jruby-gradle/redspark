#!/usr/bin/env ruby

java_import 'org.apache.spark.sql.SparkSession'

puts "LOADING THINGS"

logfile = 'build.gradle'
spark = SparkSession.builder.appName('Simple Application').getOrCreate
data = spark.read.textFile(logfile).cache()

alphas = data.filter { |line| line.contains('a') }.count
betas = data.filter { |line| line.contains('b') }.count

puts
puts "Hello from Ruby, we read #{logfile}"
puts " and found #{alphas} 'a' characters"
puts " and #{betas} 'b' characters"
puts 

spark.stop()
