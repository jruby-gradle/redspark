#!/usr/bin/env ruby

java_import 'org.apache.spark.sql.SparkSession'

logfile = 'build.gradle'
spark = SparkSession.builder.appName('Simple Application').getOrCreate
data = spark.read.textFile(logfile).cache()

alphas = data.distinct
puts "about to filter"
betas = data.filter do |line|
  puts 'filtering..'
  line.contains 'b'
end.count
puts "filtered"

puts
puts "Hello from Ruby, we read #{logfile}"
puts " and found #{alphas} 'a' characters"
puts " and #{betas} 'b' characters"
puts 

spark.stop()
