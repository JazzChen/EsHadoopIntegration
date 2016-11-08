package com.ebay.eshadoop.sparkjobs;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class HDFS { 
    
    public static void main(String[] args) { 
        try{ 
      
        //SparkConf conf=new SparkConf().setAppName("HDFS APP"); 
        	SparkConf conf = new SparkConf().setAppName("ES_SPARK").setMaster("yarn-cluster").set("es.nodes", "es-master-8136.lvs01.dev.ebayc3.com:9200");
            JavaStreamingContext ssc=new JavaStreamingContext(conf,Durations.seconds(1));//conf, Durations.seconds(1)); 
       String path=args[0]; 
            System.out.println("*************************************"); 
            System.out.println("------->>>HDFS Path is :"+path); 
            JavaDStream<String> jds=ssc.textFileStream(path); 
            
            System.out.println(" JDS: "+jds); 
    
            
        
           jds.window(Durations.minutes(2), Durations.minutes(1)); 
           jds.count().print(); 
           System.out.println(" ======================================================================================"); 
           System.out.println(" ======================================================================================");
           
           jds.foreach(new Function<JavaRDD<String>, Void>() {			
   			public Void call(JavaRDD<String> rdd) throws Exception {   				
   				System.out.println(" Totoal Count In RDD  ==> "+rdd.count());
   				System.out.println(" rdd List of Data  ==> "+rdd.collect());
   				System.out.println(" RDD ==> "+rdd.first());
   				/*List<String> list = rdd.collect();
   				HashMap[] logArray = new HashMap[list.size()];
   				for( int i=0 ; i < list.size() ; i++  )
   				{
   					HashMap<String, String> log = new HashMap<>();
   					log.put("log", list.get(i));
   					logArray[i] = log;
   				}
   				
   				JavaRDD<HashMap> javaRDD = sc.parallelize(ImmutableList.of(logArray));
   				JavaEsSpark.saveToEs(javaRDD, "spark_streaming/docs");*/
   				return null;
   			}
   		});
          
            System.out.println("@@@@@@@@@@@@@@@@@@===**==@@@@@@@@@@@@@@@@"); 
        jds.print(); 
        ssc.start(); 
        ssc.awaitTermination();
    
        
        } 
        catch(Exception e) 
        { 
            e.printStackTrace(); 
            
        } 
    } 
} 
