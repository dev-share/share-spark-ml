package com.glocalme.css.spark.util;

public class StringUtil {
	public static boolean isEmptyStr(String target){
		if(target==null||"".equals(target)||"null".equals(target)){
			return true;
		}
		return false;
	}
	
	public static String join(Iterable<?> iterable, String separator) {
		return org.apache.commons.lang.StringUtils.join(iterable.iterator(), separator);
	}
}
