package com.glocalme.share.spark.common;

import com.glocalme.share.spark.common.cassandra.CassandraFactory;
import com.glocalme.share.spark.common.greenplum.GreenplumFactory;
import com.glocalme.share.spark.common.jdbc.JDBCFactory;
import com.glocalme.share.spark.common.mongodb.MongoDBFactory;
/**
 * @decription 配置文件获取数据源工厂
 * @author yi.zhang
 * @time 2017年7月31日 下午12:03:10
 * @since 1.0
 * @jdk	1.8
 */
public class DataSourceUtil {
	/**
	 * @decription Cassandra配置
	 * @author yi.zhang
	 * @time 2017年7月31日 下午12:03:45
	 * @return
	 */
	public static CassandraFactory cassandra(){
		try {
			String servers = SparkConfig.getProperty("cassandra.servers");
			String keyspace = SparkConfig.getProperty("cassandra.keyspace");
			String username = SparkConfig.getProperty("cassandra.username");
			String password = SparkConfig.getProperty("cassandra.password");
			CassandraFactory factory = new CassandraFactory();
			factory.init(servers, keyspace, username, password);
			return factory;
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	/**
	 * @decription MongoDB配置
	 * @author yi.zhang
	 * @time 2017年7月31日 下午12:03:45
	 * @return
	 */
	public static MongoDBFactory mongodb(){
		try {
			String servers = SparkConfig.getProperty("mongodb.servers");
			String database = SparkConfig.getProperty("mongodb.database");
			String schema = SparkConfig.getProperty("mongodb.schema");
			String username = SparkConfig.getProperty("mongodb.username");
			String password = SparkConfig.getProperty("mongodb.password");
			MongoDBFactory factory = new MongoDBFactory();
			factory.init(servers, database, schema, username, password);
			return factory;
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	
	/**
	 * @decription Greenplum配置
	 * @author yi.zhang
	 * @time 2017年7月31日 下午12:03:45
	 * @return
	 */
	public static GreenplumFactory greenplum(){
		try {
			String address = SparkConfig.getProperty("greenplum.address");
			String database = SparkConfig.getProperty("greenplum.database");
			String schema = SparkConfig.getProperty("greenplum.schema");
			String username = SparkConfig.getProperty("greenplum.username");
			String password = SparkConfig.getProperty("greenplum.password");
			boolean isDruid = Boolean.valueOf(SparkConfig.getProperty("jdbc.druid.enabled"));
			Integer max_pool_size = Integer.valueOf(SparkConfig.getProperty("jdbc.druid.max_pool_size"));
			Integer init_pool_size = Integer.valueOf(SparkConfig.getProperty("jdbc.druid.init_pool_size"));
			GreenplumFactory factory = new GreenplumFactory();
			factory.init(address, database, schema, username, password, isDruid, max_pool_size, init_pool_size);
			return factory;
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	/**
	 * @decription JDBC(MySQL|SQL Server|Oracle等)配置
	 * @author yi.zhang
	 * @time 2017年7月31日 下午12:03:45
	 * @return
	 */
	public static JDBCFactory jdbc(){
		try {
			String driverName = SparkConfig.getProperty("jdbc.driver");
			String url = SparkConfig.getProperty("jdbc.url");
			String username = SparkConfig.getProperty("jdbc.username");
			String password = SparkConfig.getProperty("jdbc.password");
			boolean isDruid = Boolean.valueOf(SparkConfig.getProperty("jdbc.druid.enabled"));
			Integer max_pool_size = Integer.valueOf(SparkConfig.getProperty("jdbc.druid.max_pool_size"));
			Integer init_pool_size = Integer.valueOf(SparkConfig.getProperty("jdbc.druid.init_pool_size"));
			JDBCFactory factory = new JDBCFactory();
			factory.init(driverName, url, username, password, isDruid, max_pool_size, init_pool_size);
			return factory;
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
}
