package com.ebay.eshadoop.sparkjobs;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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


/**
 * Polling Spark Job which polls Spark Sink which will be in Central Flume Aggregator
 * 
 * This create Polling Streaming Using FlumeUtils.createPollingStream
 * Connects the Flume Agent Machine where Spark Sink running and Reads the data 
 * 
 * When it reads the data it will send the ACK to Spark Sink to remove the data completely from Flume Channel (memory)
 * In case of any errors Spark Job will send NACK and re read them
 * 
 * After reading the data this Class aggregate and writes to Elastic Search
 * <steps>
 * 1. Connect Flume Spark Sink Machine
 * 2. Poll specified interval
 * 3. Read data from Spark Sink
 * 4. Aggregate 
 * 5. Writes to ElasticSearch
 * </steps>
 * 
 * @author senthikumar@ebay.com
 *
 */
public class FlumePollingStream {
	
	static Map<Integer , Integer > minutesSlot;
	static int minutesInterval;
	static String logLevel;
	static String log4jRegEx = null;
	static Pattern pattern = null;
	static String esIndexName = null;
	static ExceptionTypeMapping exceptionMapper;
	static String logType;
	
	@SuppressWarnings({"serial"})
	public static void main(String[] args) throws  Exception {
		
		// arg[0] == yarn-client or yarn-cluster
		// arg[1] == ElasticSeatch Node 
		// arg[2] == Flume Log Aggregator Host { host:port,host:port }
		
		
		// conf.get("spark.flumestreaming.streaming_batch_interval") 
		// Spark has intelligent to see properties in SparkConfig object
		// if user passes the .properties file using --properties-file option in SPARK SUBMIT ( note property should start with spark.) 
		
		SparkConf conf = new SparkConf().setMaster(args[0]);
		conf.set("es.nodes", args[1]);
		System.out.println("spark.flumestreaming.streaming_batch_interval = "+conf.get("spark.flumestreaming.streaming_batch_interval"));
		System.out.println("spark.flumestreaming.streaming_window_interval = "+conf.get("spark.flumestreaming.streaming_window_interval"));
		System.out.println("spark.flumestreaming.streaming_batch_size = "+conf.get("spark.flumestreaming.streaming_batch_size"));
		System.out.println("spark.flumestreaming.streaming_parallel_threads = "+conf.get("spark.flumestreaming.streaming_parallel_threads"));
		System.out.println("spark.flumestreaming.exceptionmapping = "+conf.get("spark.flumestreaming.exceptionmapping"));
		
		// Log Level from argument
		logLevel = args[3];
		
		minutesSlot = StreamingUtils.constructMinutesSlot(  Integer.parseInt(conf.get("spark.flumestreaming.streaming_batch_interval")) /60);
		exceptionMapper = new ExceptionTypeMapping();
		exceptionMapper.parseExceptionTypes(conf.get("spark.flumestreaming.exceptionmapping"));
		pattern = Pattern.compile(conf.get("spark.flumestreaming.regexp").trim());
		conf.setAppName(conf.get("spark.flumestreaming.appname"));
		final JavaSparkContext sc = new JavaSparkContext(conf);
		// create java streaming context 
		JavaStreamingContext jssc = new JavaStreamingContext(sc, Durations.seconds( Integer.parseInt(conf.get("spark.flumestreaming.streaming_batch_interval")) ) );
		
		// Poll Stream 
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
				long rddCount = rdd.count();
				System.out.println("No Of Events in RDD "+rddCount);
				if( rddCount > 0 )
				{
					final List<SparkFlumeEvent> logEventlist = rdd.collect(); // Collect all Events from RDD
					Map< LogAggregationKey , Integer > log = new HashMap<LogAggregationKey, Integer>();
					for(SparkFlumeEvent sparkEvent : logEventlist )
					{
						AvroFlumeEvent event = sparkEvent.event();
						// look for the HEADERS in Flume Event , if it exsists set type else set default 
						logType =  event.getHeaders().get("LOGTYPE") != null ? (String) event.getHeaders().get("LOGTYPE") : "nnlogs";
						String logEvent  = new String(event.getBody().array());
						filterAndAggregateEvents(log, logEvent);
					}					
					final List<Map<Object,Object>>  finalESDocsList = generateFinalESDocs( log );
					System.out.println(" No of Documents eligible for Indexing ==> "+finalESDocsList.size());
					
					if( finalESDocsList.size() > 0 )
					{
						System.out.println(" Es Index Name  ==> "+esIndexName);
						/*
						 * JavaEsSpark.saveToEs API connects  es.nodes and write the Log Events directly to ES
						 * Refer  elasticsearch-spark module for Other Supported API's
						 */
						JavaEsSpark.saveToEs(sc.parallelize(finalESDocsList), esIndexName);
					}
				}				
				rdd.unpersist();
				return null;			
			}		
			});		
		jssc.start();
		jssc.awaitTermination();
	}
	
	
	/**
	 * Convert Aggregated Map to ES documents 
	 * @param logMap
	 * @return listOfEsDocs
	 */
	public static List<Map<Object,Object>> generateFinalESDocs( Map< LogAggregationKey , Integer > logMap )
	{
		List<Map<Object,Object>> finalESDocs = new ArrayList<Map<Object,Object>>();		
		for( LogAggregationKey keyObj : logMap.keySet() )
		{
			Map<Object , Object > esDoc = new HashMap<Object, Object>();
			esDoc.put("date", keyObj.getDate());
			esDoc.put("hour", keyObj.getHour());
			esDoc.put("minutes", keyObj.getMinutes());
			esDoc.put("exception_type", keyObj.getExceptionType());
			esDoc.put("exception_count", logMap.get(keyObj));
			if( keyObj.getUser() != null && keyObj.getUser().length() > 0 )
				esDoc.put("user", keyObj.getUser());
			
			System.out.println(" esDoc Map ==> "+esDoc);
			finalESDocs.add(esDoc);
		}
		return finalESDocs;
	}
		
	
	/**
	 * Filter and Aggregate the events based on date/time/exception type 
	 * @param log
	 * @param logEvent
	 */
	
	public static void filterAndAggregateEvents( Map< LogAggregationKey , Integer > logMap , String logEvent )
	{		
		Matcher m = pattern.matcher(logEvent);		
		if( m.find() &&  logLevel.equalsIgnoreCase(m.group(3)) )
		{	
			System.out.println(" Pattern Matched ");
			String[] tempArr = m.group(2).split(",");
			Integer logMinz = Integer.parseInt(tempArr[0].split(":")[1]);
			String date = m.group(1)+"T"+Integer.valueOf(tempArr[0].split(":")[0])+":"+Integer.valueOf(minutesSlot.get(logMinz));
			Integer hour = Integer.valueOf(tempArr[0].split(":")[0]);
			Integer minutes = Integer.valueOf(minutesSlot.get(logMinz));
			esIndexName = m.group(1)+"/"+logType;			
			LogAggregationKey key = new LogAggregationKey();
			key.setDate(date);
			key.setHour(hour);
			key.setMinutes(minutes);
			key.setExceptionType(exceptionMapper.getExcpetionType(m.group(5)));
			// Only for NameSpace Quota Violation			
			if( key.getExceptionType().equalsIgnoreCase("NameSpaceQuotaViolation"))
			{
				// REVISIT below four lines .. This should be dynamic
				String tmp = m.group(5).substring(m.group(5).indexOf("for"), m.group(5).indexOf("", m.group(5).indexOf("quota =")));
				tmp= tmp.substring(tmp.indexOf(" ")).trim();
				tmp = tmp.replaceAll("/user/", "");
				key.setUser(tmp);
			}
			// Check the key already exists 
			if(  logMap.containsKey(key) )
				logMap.put( key, (logMap.get(key)+1) );
			else
				logMap.put( key, 1 );			
		}
	}
	
}
