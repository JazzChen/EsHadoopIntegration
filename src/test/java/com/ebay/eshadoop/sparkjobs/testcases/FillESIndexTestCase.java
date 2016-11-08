package com.ebay.eshadoop.sparkjobs.testcases;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import com.ebay.eshadoop.sparkjobs.FlumePollingStream;
import com.ebay.eshadoop.sparkjobs.LogAggregationKey;

public class FillESIndexTestCase {

	@Test
	public void testFillESIndex()
	{
		Map< LogAggregationKey , Integer > log = new HashMap<LogAggregationKey, Integer>();
		String logEvent="2015-05-25 00:12:26,246 ERROR org.apache.hadoop.hdfs.server.namenode.FSImage: BUG: Namespace quota violation in image for /user/kbalaraju quota = 16384 < consumed = 16456";
		//FlumePollingStream.filterAndAggregateEvents(log, logEvent);
		//FlumePollingStream.filterAndAggregateEvents(log, logEvent);
	}
}
