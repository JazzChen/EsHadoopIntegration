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




public class SocketStreaming {

	@SuppressWarnings("serial")
	public static void main(String[] args) {
		
		/*SparkConf conf = new SparkConf().setAppName("ES_SPARK").setMaster("yarn-cluster").set("es.nodes", "es-master-8136.lvs01.dev.ebayc3.com:9200");
		//JavaStreamingContext streamingContext = new JavaStreamingContex
		//StreamingContext context = new S
		spark.SparkContext sC = new SparkContext(conf);
		Duration interval = new Duration(10000);
		StreamingContext streamCtx = new StreamingContext(sC, interval);*/
		// 
		
		//hdfs://10.64.217.106:8020/user/senthikumar/spark/streaming/test
		//spark://es-hdfs-int-nn-6008:7077
		//yarn-cluster
		SparkConf conf = new SparkConf().setAppName("ES_SPARK").setMaster("yarn-client").set("es.nodes", "es-master-8136.lvs01.dev.ebayc3.com:9200");
		final JavaSparkContext sc = new JavaSparkContext(conf);
		// final JavaStreamingContext jssc = new JavaStreamingContext(sc, new Duration(1000));
		final JavaStreamingContext jssc = new JavaStreamingContext(sc, new Duration(1000*60*5) );
		// Check Point 
		//jssc.checkpoint("hdfs://10.64.217.106:8020/tmp/spark/streaming-logs");
		// hdfs://10.64.217.106:8020/user/senthikumar/spark/streaming/logs
		//JavaDStream<String> lines = jssc.textFileStream("hdfs://localhost:9000//user/senthilkumar/spark/streamdir/");
		//JavaDStream<String> lines = jssc.textFileStream("hdfs://localhost:9000//user/senthilkumar/spark/streamdir/");
		//10.64.217.130
		//JavaDStream<String> lines = jssc.socketTextStream("10.64.217.130", 4712);
		JavaDStream<String> lines = jssc.socketTextStream("10.64.217.106", 4712);
		lines.window(new Duration(1000*60*5), new Duration(1000*60*5));
		//	System.out.println(lines.toString());
		/*JavaDStream<String> words = lines.flatMap(
				new FlatMapFunction<String, String>() {
					
					@Override
					public Iterable<String> call(String str) throws Exception {		
						System.out.println(" ================================================ ");
						String[] tmp = str.split(" ");
						for( int i =0; i < tmp.length ; i++ )
						{
							System.out.println("    Steaming File Contents "+tmp[i]);
						}
						return Arrays.asList(str.split(" "));
					}
				});*/
		lines.print();
		
		lines.foreach(new Function<JavaRDD<String>, Void>() {			
			public Void call(JavaRDD<String> rdd) throws Exception {
				//rdd.saveAsTextFile("hdfs://localhost:9000//user/senthilkumar/sparkstreamingout/");
				System.out.println(" Totoal Count In RDD  ==> "+rdd.count());
				System.out.println(" rdd List of Data  ==> "+rdd.collect());
				List<String> list = rdd.collect();
				HashMap[] logArray = new HashMap[list.size()];
				for( int i=0 ; i < list.size() ; i++  )
				{
					HashMap<String, String> log = new HashMap<String, String>();
					log.put("log", list.get(i));
					logArray[i] = log;
				}
				JavaEsSpark.saveToEs(sc.parallelize(ImmutableList.of(logArray)), "spark/docs");
				rdd.unpersist();
				return null;
			}
		});
		
		jssc.start();
		jssc.awaitTermination();
		
		
	}
}
