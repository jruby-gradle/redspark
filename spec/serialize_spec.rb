require 'rspec'

java_import 'org.apache.spark.sql.SparkSession'

# Inspired by test code found here:
#  http://codewicca.org/the-inevitable-task-not-serializable-sparkexception

describe 'Serializing Ruby for Spark' do
  context 'with an object' do
    let(:spark) do
      SparkSession
        .builder
        .master('local[*]')
        .appName('rspec')
        .getOrCreate
    end

    after(:each) do
      spark.stop()
    end

    it 'should serialize and execute properly' do
      data = spark.read.textFile(__FILE__).cache()
      expect {
        data.filter { |line| line.contains('a').to_java }.count
      }.not_to raise_error
    end
  end
end
