package com.ebay.eshadoop.sparkjobs;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.flume.source.avro.AvroFlumeEvent;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.flume.FlumeUtils;
import org.apache.spark.streaming.flume.SparkFlumeEvent;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;

import com.google.common.collect.ImmutableList;

public class FlumePollingStreamPipeLineTest {
	
	static String indexName ;
	@SuppressWarnings({"serial"})
	public static void main(String[] args) throws  Exception {
		
		// arg[0] == yarn-client or yarn-cluster
		// arg[1] == ElasticSeatch Node 
		// arg[2] == Flume Log Aggregator Host { host:port,host:port }
		// arg[3] == ElaticSearch Index Name 
		indexName = args[3];
		SparkConf conf = new SparkConf().setAppName( " Spark - Flume Streaming ").setMaster(args[0]);
		System.out.println("spark.flumestreaming.streaming_batch_interval = "+conf.get("spark.flumestreaming.streaming_batch_interval"));
		System.out.println("spark.flumestreaming.streaming_window_interval = "+conf.get("spark.flumestreaming.streaming_window_interval"));
		System.out.println("spark.flumestreaming.streaming_batch_size = "+conf.get("spark.flumestreaming.streaming_batch_size"));
		System.out.println("spark.flumestreaming.streaming_parallel_threads = "+conf.get("spark.flumestreaming.streaming_parallel_threads"));
		System.out.println("spark.flumestreaming.exceptionmapping = "+conf.get("spark.flumestreaming.exceptionmapping"));
		
		conf.set("es.nodes", args[1]);
		
		final JavaSparkContext sc = new JavaSparkContext(conf);		
		JavaStreamingContext jssc = new JavaStreamingContext(sc, Durations.seconds( Integer.parseInt(conf.get("spark.flumestreaming.streaming_batch_interval")) ) );
		JavaDStream<SparkFlumeEvent> lines  = FlumeUtils.createPollingStream(
														/** Spark Context  **/ jssc,
														/** Array of Spark Sink SocketAddr */ StreamingUtils.parseCommand(args[2]) ,
														/** Storage Level */ StorageLevel.MEMORY_AND_DISK() ,
														/** batchSize */ Integer.parseInt(conf.get("spark.flumestreaming.streaming_batch_size")) , 
														/**  parallel threads **/  Integer.parseInt(conf.get("spark.flumestreaming.streaming_parallel_threads"))  
														);	
		
		lines.window( Durations.seconds( Integer.parseInt(conf.get("spark.flumestreaming.streaming_window_interval")) ),  
				      Durations.seconds( Integer.parseInt(conf.get("spark.flumestreaming.streaming_window_interval"))) );		
		lines.foreach(new Function<JavaRDD<SparkFlumeEvent>, Void>() {
		
			
			public Void call(JavaRDD<SparkFlumeEvent> rdd) throws Exception {
				System.out.println("No Of Events in RDD "+rdd.count());
				if( rdd.count() > 0 )
				{
					List<SparkFlumeEvent> list = rdd.collect();
					final List< Map<Object, Object> > logsList  = new ArrayList<Map<Object, Object>>();
					for( int i=0 ; i < list.size() ; i++  )
					{
						AvroFlumeEvent event = list.get(i).event();			
						String logEvent  = new String(event.getBody().array());
						Map<Object, Object> log = new HashMap<Object, Object>();
						log.put("log", logEvent);
						logsList.add(log);
					}
					System.out.println(" No of Documents eligible for Indexing ==> "+logsList.size());
					
					if( logsList.size() > 0 )
					{
						System.out.println(" Es Index Name  ==> "+indexName);
						JavaEsSpark.saveToEs(sc.parallelize(logsList), indexName);						
					}
				}				
				rdd.unpersist();
				return null;			
			}		
			});		
		jssc.start();
		jssc.awaitTermination();
	}
	
	
}
