package com.ebay.eshadoop.sparkjobs;

public enum StreamingInput {

	APP_NAME{ @Override public String toString() {return "appname";}},
	REG_EXP{ @Override public String toString() {return "regexp";}},
	BATCH_INTERVAL{ @Override public String toString() {return "streaming_batch_interval";}},
	WINDOW_INTERVAL{ @Override public String toString() {return "streaming_window_interval";}},
	NO_OF_PARALLEL_THREADS{ @Override public String toString() {return "streaming_parallel_threads";}},
	BATCH_SIZE{ @Override public String toString() {return "streaming_batch_size";}}
	
}
