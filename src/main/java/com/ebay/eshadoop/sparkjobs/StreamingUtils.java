package com.ebay.eshadoop.sparkjobs;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.rmi.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class StreamingUtils {

	static Properties _props = null;
	static String propsPath ;
	/**
	 * 
	 * @return
	 */
	public static Map<Integer , Integer > constructMinutesSlot(Integer minutesInterval )
	{
		Map<Integer , Integer > minutesSlot = new HashMap <Integer , Integer >();
		int temp =00 ; 
		for( int i = 0; i < 60 ; i++ )
		{
			if( i % minutesInterval == 0 )
				temp = i;
			
			minutesSlot.put(i, temp );
		}
		return minutesSlot;		
	}
	
	
	
	public static InetSocketAddress[] parseCommand( String hosts ) throws Exception
	{
		// 
		if( null == hosts )
			throw new UnknownHostException(" Passed Host  "+hosts+" is not Valid ");
		
		String[] hostsArr = hosts.split(",");
		
		InetSocketAddress[] inetSocketArr = new InetSocketAddress[hostsArr.length];
		
		for ( int i =0 ; i < hostsArr.length ; i++ )
		{
			String[] hostArr = hostsArr[i].split(":");
			System.out.println(" Socket Host "+hostArr[0]+" Port "+hostArr[1]);
			InetSocketAddress socketAddr = new InetSocketAddress(hostArr[0] , Integer.parseInt(hostArr[1]));
			inetSocketArr[i] = socketAddr;
		}
		
		return inetSocketArr;
	}
	
	/**
	 * Load the Spark Job Input Properties
	 * @return
	 * @throws IOException
	 */
	
	public static Properties loadStreamingJobInput() throws IOException
	{
		
		System.out.println(" Loading File from Path : "+propsPath);
		File file = new File("streaming_input.properties");
		System.out.println(" File Exsits ? "+file.exists());
		InputStream in = new FileInputStream(file);
		_props.load(in);
		in.close();
		System.out.println(" Properties loaded Successfully ");
		System.out.println(_props.keySet());
		return _props;
	}
	
	
	public static  String getStringValue( String key )
	{
		if ( null == key )
			return null;
		
		
		return getProperties().getProperty(key.trim());
	}
	
	
	public static Integer getIntval( String key )
	{
		if ( null == key )
			return 0;
		
		return Integer.parseInt( getProperties().getProperty(key.trim()) );
	}
	
	public static Properties getProperties() 
	{
		if( _props == null )
			try {
				loadStreamingJobInput();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		
		return _props;
	}
	
}
