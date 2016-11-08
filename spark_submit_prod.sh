
# Copy & Paste this in Spark Machine 

/apache/spark/bin/spark-submit --master "yarn"  --deploy-mode "cluster" --class com.ebay.eshadoop.sparkjobs.FlumePollingStreamPipeLineTest --properties-file /home/hadoop/streaming_input.properties  --driver-class-path "/apache/hadoop/conf:/apache/hadoop/share/hadoop/common/hadoop-common-2.4.1-EBAY-21.jar:/apache/hadoop/share/hadoop/hdfs/hadoop-hdfs-2.4.1-EBAY-21.jar:/apache/hadoop/share/hadoop/common/lib/hadoop-ebay-2.4.1-EBAY-11.jar:/apache/hadoop/lib/hadoop-lzo-0.6.0.jar:/apache/spark/lib/sparklibs/guava-18.0.jar"  --jars "/apache/spark/lib/sparksubmit_libs/spark-streaming_2.10-1.4.1.jar,/apache/spark/lib/sparksubmit_libs/spark-streaming-flume_2.10-1.4.1.jar,/apache/spark/lib/sparksubmit_libs/spark-streaming-flume-sink_2.10-1.5.0.jar,/apache/spark/lib/sparklibs/flume-ng-sdk-1.3.1.jar,/apache/spark/lib/sparklibs/guava-18.0.jar,/apache/spark/lib/sparksubmit_libs/elasticsearch-spark_2.10-2.1.0.jar,/apache/hadoop/share/hadoop/common/hadoop-common-2.4.1-EBAY-21.jar,/apache//hadoop/share/hadoop/hdfs/hadoop-hdfs-2.4.1-EBAY-21.jar,/apache/hadoop/share/hadoop/common/lib/hadoop-ebay-2.4.1-EBAY-11.jar,/apache/hadoop/lib/hadoop-lzo-0.6.0.jar" --conf spark.driver.userClassPathFirst=true --executor-memory "512m" --verbose --executor-cores 1 --queue "hdlq-other-default" --num-executors 3 /home/hadoop/spark_deployment/elasticsearch_hadoop_integration-0.0.1.jar yarn-cluster  phxapdes0002.stratus.phx.ebay.com:9200 lvsaishc3dn1187.stratus.lvs.ebay.com:4712 eshdfs_sep30/nnlogs


# Spark Submit Command Production Version
/apache/spark/bin/spark-submit  
--master "yarn"  
--deploy-mode "cluster" 
--class com.ebay.eshadoop.sparkjobs.FlumePollingStreamPipeLineTest
--properties-file /home/hadoop/streaming_input.properties 
--driver-class-path "/apache/hadoop/conf:/apache/hadoop/share/hadoop/common/hadoop-common-2.4.1-EBAY-21.jar:/apache/hadoop/share/hadoop/hdfs/hadoop-hdfs-2.4.1-EBAY-21.jar:/apache/hadoop/share/hadoop/common/lib/hadoop-ebay-2.4.1-EBAY-11.jar:/apache/hadoop/lib/hadoop-lzo-0.6.0.jar:/apache/spark/lib/sparklibs/guava-18.0.jar" 
--jars "/apache/spark/lib/sparksubmit_libs/spark-streaming_2.10-1.4.1.jar,
		/apache/spark/lib/sparksubmit_libs/spark-streaming-flume_2.10-1.4.1.jar,
		/apache/spark/lib/sparksubmit_libs/spark-streaming-flume-sink_2.10-1.5.0.jar,
		/apache/spark/lib/sparklibs/flume-ng-sdk-1.3.1.jar,
		/apache/spark/lib/sparklibs/guava-18.0.jar,
		/apache/spark/lib/sparksubmit_libs/elasticsearch-spark_2.10-2.1.0.jar,
		/apache/hadoop/share/hadoop/common/hadoop-common-2.4.1-EBAY-21.jar,
		/apache//hadoop/share/hadoop/hdfs/hadoop-hdfs-2.4.1-EBAY-21.jar,
		/apache/hadoop/share/hadoop/common/lib/hadoop-ebay-2.4.1-EBAY-11.jar,
		/apache/hadoop/lib/hadoop-lzo-0.6.0.jar"
--conf spark.driver.userClassPathFirst=true 
--executor-memory "512m" 
--verbose 
--executor-cores 1  
--queue "hdlq-other-default" 
--num-executors 3 
/home/hadoop/spark_deployment/elasticsearch_hadoop_integration-0.0.1.jar
yarn-cluster 
phxapdes0002.stratus.phx.ebay.com:9200 
lvsaishc3dn1187.stratus.lvs.ebay.com:4712
eshdfs_sep30/nnlogs
