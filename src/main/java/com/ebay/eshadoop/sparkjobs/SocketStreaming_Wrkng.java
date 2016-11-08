package com.ebay.eshadoop.sparkjobs;

import java.util.HashMap;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;

import com.google.common.collect.ImmutableList;

public class SocketStreaming_Wrkng {

	@SuppressWarnings("serial")
	public static void main(String[] args) {		
		//yarn-cluster
		SparkConf conf = new SparkConf().setAppName("ES_SPARK");
		conf.setMaster("yarn-client"); // yarn-cluster or yarn-client or spark:master
		conf.set("es.nodes", "es-master-8136.lvs01.dev.ebayc3.com:9200"); // Elastic Search Node
		final JavaSparkContext sc = new JavaSparkContext(conf);
		final JavaStreamingContext jssc = new JavaStreamingContext(sc, new Duration(1000*60*5) ); // Duration is Batch Interval
		JavaDStream<String> lines = jssc.socketTextStream("CentralizedLogCollectorHost", 4712);
		lines.window(new Duration(1000*60*5), new Duration(1000*60*5));	 
		// Window is basically Wait for specified interval (i.e ) keep all events in memory 
		// and once the time reaches the limit start processing the stream as batch 
		lines.foreach(new Function<JavaRDD<String>, Void>() {			
			public Void call(JavaRDD<String> rdd) throws Exception {
				//rdd.saveAsTextFile("HDFS PATH "); // Optional
				System.out.println(" Totoal Count In RDD  ==> "+rdd.count());				
				List<String> list = rdd.collect();				
				HashMap[] logArray = new HashMap[list.size()];
				for( int i=0 ; i < list.size() ; i++  )
				{
					HashMap<String, String> log = new HashMap<String, String>();
					log.put("log", list.get(i));
					logArray[i] = log;
				}				
				JavaEsSpark.saveToEs(sc.parallelize(ImmutableList.of(logArray)), "spark/c");
				rdd.unpersist();
				return null;
			}
		});
		jssc.start();
		jssc.awaitTermination();
		jssc.stop();
		jssc.close();
		
	}
}
