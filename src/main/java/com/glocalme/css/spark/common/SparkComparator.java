package com.glocalme.css.spark.common;

import java.io.Serializable;
import java.util.Comparator;
/**
 * @decription Spark比较序列化
 * @author yi.zhang
 * @time 2017年7月13日 上午11:50:11
 * @since 1.0
 * @jdk	1.8
 * @param <T>
 */
public interface SparkComparator<T> extends Comparator<T>, Serializable {

}
