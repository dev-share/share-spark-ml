package com.glocalme.share.spark.common;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class SparkConfig {
	private static Logger log = LogManager.getLogger(SparkConfig.class);
	private static Properties config = null;
	
	public static Properties getInstance(String properties){
		InputStream in = null; 
		try {
			in = ClassLoader.getSystemResourceAsStream(properties);
			config = new Properties();
			config.load(in);
		} catch (IOException e) {
			log.error("--Spark Properties read error!",e);
		}finally{
			if(in!=null){
				try {
					in.close();
				} catch (Exception e) {
					log.error("--Spark InputStream read error!",e);
				}
			}
		}
		return config;
	}
	
	public static String getProperty(String key){
		if(config==null){
			config = getInstance("spark.properties");
		}
		return config.getProperty(key);
	}
}
