package com.ebay.eshadoop.sparkjobs;

import java.io.IOException;

import org.apache.spark.SparkConf;

public class SparkAvro {
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static void main(String[] args) throws IOException {
		
		SparkConf conf = new SparkConf().setAppName("SPARK_AVRO").setMaster("yarn-client");
		conf.set("spark.serializer","org.apache.spark.serializer.KryoSerializer");
		//conf.set("spark.kryo.registrator", com.ebay.avro.student_marks.class.toString());
		conf.set("spark.kryo.registrationRequired", "true");
		Class[] classes = new Class[1];
		/*classes[0] = com.ebay.avro.student_marks.class;
		//classes[1] = scala.Tuple2.class;
		conf.registerKryoClasses(classes);
		JobConf readAvroJobConf = new JobConf();
		
		AvroJob.setInputSchema(readAvroJobConf, student_marks.getClassSchema() );
		SparkContext sc = new SparkContext(conf);
		System.out.println(" Counting AVRO RDDs "+sc.newAPIHadoopFile("/user/senthilkumar/avro/", AvroKeyInputFormat.class, AvroKey.class, NullWritable.class, readAvroJobConf).count());
		*/
	}

}
