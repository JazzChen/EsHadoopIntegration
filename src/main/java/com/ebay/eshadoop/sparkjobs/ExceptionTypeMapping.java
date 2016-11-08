package com.ebay.eshadoop.sparkjobs;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;


/**
 * This class responsible for mapping the exceptions of the event received
 * @author senthikumar@ebay.com
 *
 */
public class ExceptionTypeMapping {
	
	
	public static final Map<String,String> exceptionTypesMap = new HashMap<String,String>();
	
	
	/**
	 * Parse and store exception mapping in Map
	 * @param exceptions
	 */
	public void parseExceptionTypes( String exceptions )
	{
		if( null == exceptions )
			return;
		System.out.println(" exceptions ==> "+exceptions);
		String[] tempArr = exceptions.split(":");		
		for( int i =0 ; i < tempArr.length ; i++ )
		{
			System.out.println(" Exceptions  ==> "+tempArr[i]);
			String[] exMappingArr = tempArr[i].split("~");
			System.out.println(" ExceptionMap ==> "+exMappingArr[0] +"   "+exMappingArr[1]);
			exceptionTypesMap.put(exMappingArr[1], exMappingArr[0]);
			System.out.println(exceptionTypesMap);
		}
	}
	
	/**
	 * Check the exception Str matches excpetionTypesMap if it matches return appropriate Exception 
	 * @param exceptionStr
	 * @return
	 */
	public String getExcpetionType( String exceptionStr  )
	{
		String exceptionType = "OTHERS" ;
		if( null ==  exceptionStr )
			return null;
		
		System.out.println(" Exception String  ==> "+exceptionStr);		
		Set<String> keySet = exceptionTypesMap.keySet();
		
		for( String key : keySet)
		{
			System.out.println(" Key ==> "+ key);
			System.out.println(" Exception String  ==> "+exceptionStr);
			System.out.println(" Contains check "+exceptionStr.contains(key));
			// check the key contains 
			if( exceptionStr.contains(key) )
				return exceptionTypesMap.get(key);
			
		}		
		return exceptionType;
	}
}
