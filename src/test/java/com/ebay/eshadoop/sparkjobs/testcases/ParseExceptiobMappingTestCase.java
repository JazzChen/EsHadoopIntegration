package com.ebay.eshadoop.sparkjobs.testcases;

import org.junit.Test;

import com.ebay.eshadoop.sparkjobs.ExceptionTypeMapping;

public class ParseExceptiobMappingTestCase {

	@Test
	public void testExcpetionParser()
	{
		ExceptionTypeMapping mapping = new ExceptionTypeMapping();
		//mapping.parseExceptionTypes("NameSpaceQuotaViolation~quota violation in image:CheckPointException~Exception in doCheckPoint");
		//mapping.getExcpetionType("BUG: Namespace quota violation in image for /user/prathykumar quota = 16384 < consumed = 29864");
	}
}
