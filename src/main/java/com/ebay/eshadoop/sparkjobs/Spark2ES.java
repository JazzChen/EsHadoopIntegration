package com.ebay.eshadoop.sparkjobs;

import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

// spark://es-hdfs-int-nn-6008:7077
public class Spark2ES {
	
	public static void main(String[] args) {
		
		//spark://localhost:18080
		//10.64.217.106
		SparkConf conf = new SparkConf().setAppName("ES_SPARK").setMaster("yarn-cluster").set("es.nodes", "es-master-8136.lvs01.dev.ebayc3.com:9200");
		conf.set("es.index.auto.create", "true");
		JavaSparkContext jsc = new JavaSparkContext(conf);		
		/*TripBean upcoming = new TripBean("OTP", "SFO");
		TripBean lastWeek = new TripBean("MUC", "OTP");	*/
		
		Map<String, ?> numbers = ImmutableMap.of("one", 1, "two", 2);                   
		Map<String, ?> airports = ImmutableMap.of("OTP", "Otopeni", "SFO", "San Fran");
		
		JavaRDD<Map<String, ?>> javaRDD = jsc.parallelize(ImmutableList.of(numbers,airports));
		JavaEsSpark.saveToEs(javaRDD, "spark/docs");
		// 10.64.217.106
	}
}
