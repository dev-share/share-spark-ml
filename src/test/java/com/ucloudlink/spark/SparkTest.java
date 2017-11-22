package com.ucloudlink.spark;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;

import com.alibaba.fastjson.JSON;
public class SparkTest {
	public static void test1(String[] args){
		// context ，用于读文件 ，类似于scala的sc
				// 格式为：
				// JavaSparkContext(master: String, appName: String, sparkHome: String,
				// jars: Array[String], environment: Map[String, String])
		JavaSparkContext ctx = new JavaSparkContext("yarn-standalone", "JavaWordCount", System.getenv("SPARK_HOME"),
				JavaSparkContext.jarOfClass(SparkTest.class));

		// 也可以使用ctx获取环境变量，例如下面的语句
		System.out.println("spark home:" + ctx.getSparkHome());
		// 一次一行，String类型 ,还有hadoopfile，sequenceFile什么的 ，可以直接用sc.textFile("path")
		JavaRDD<String> lines = ctx.textFile(args[1], 1); // java.lang.String
															// path, int
															// minSplits
		lines.cache(); // cache，暂时放在缓存中，一般用于哪些可能需要多次使用的RDD，据说这样会减少运行时间
		// collect方法，用于将RDD类型转化为java基本类型，如下
		List<String> line = lines.collect();
		for (String val : line)
			System.out.println(val);
		// 下面这些也是RDD的常用函数
		// lines.collect(); List<String>
		// lines.union(); javaRDD<String>
		// lines.top(1); List<String>
		// lines.count(); long
		// lines.countByValue();
		/**
		 * filter test 定义一个返回bool类型的函数，spark运行filter的时候会过滤掉那些返回只为false的数据 String
		 * s，中的变量s可以认为就是变量lines（lines可以理解为一系列的String类型数据）的每一条数据
		 */
		JavaRDD<String> contaninsE = lines.filter(new Function<String, Boolean>() {
			@Override
			public Boolean call(String s) throws Exception {

				return (s.contains("they"));
			}
		});
		System.out.println("--------------next filter's  result------------------");
		line = contaninsE.collect();
		for (String val : line)
			System.out.println(val);
		/**
		 * sample test sample函数使用很简单，用于对数据进行抽样 参数为：withReplacement: Boolean,
		 * fraction: Double, seed: Int
		 * 
		 */
		JavaRDD<String> sampletest = lines.sample(false, 0.1, 5);
		System.out.println("-------------next sample-------------------");
		line = sampletest.collect();
		for (String val : line)
			System.out.println(val);
		/**
		 * 
		 * new FlatMapFunction<String, String>两个string分别代表输入和输出类型
		 * Override的call方法需要自己实现一个转换的方法，并返回一个Iterable的结构
		 * 
		 * flatmap属于一类非常常用的spark函数，简单的说作用就是将一条rdd数据使用你定义的函数给分解成多条rdd数据
		 * 例如，当前状态下，lines这个rdd类型的变量中，每一条数据都是一行String，我们现在想把他拆分成1个个的词的话， 可以这样写 ：
		 */
		JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
			@Override
			public Iterator<String> call(String s) {
				String[] words = s.split(" ");
				return (Iterator<String>) Arrays.asList(words);
			}
		});
		/**
		 * map 键值对 ，类似于MR的map方法 pairFunction<T,K,V>: T:输入类型；K,V：输出键值对
		 * 需要重写call方法实现转换
		 */
		// JavaPairRDD<String, Integer> ones = words.map(new
		// PairFunction<String, String, Integer>() {
		// @Override
		// public Tuple2<String, Integer> call(String s) {
		// return new Tuple2<String, Integer>(s, 1);
		// }
		// });
		// A two-argument function that takes arguments
		// of type T1 and T2 and returns an R.
		/**
		 * reduceByKey方法，类似于MR的reduce
		 * 要求被操作的数据（即下面实例中的ones）是KV键值对形式，该方法会按照key相同的进行聚合，在两两运算
		 */
		// JavaPairRDD<String, Integer> counts = ones.reduceByKey(new
		// Function2<Integer, Integer, Integer>() {
		// @Override
		// public Integer call(Integer i1, Integer i2) {
		// //reduce阶段，key相同的value怎么处理的问题
		// return i1 + i2;
		// }
		// });
		// 备注：spark也有reduce方法，输入数据是RDD类型就可以，不需要键值对，
		// reduce方法会对输入进来的所有数据进行两两运算
		/**
		 * sort，顾名思义，排序
		 */
		// JavaPairRDD<String,Integer> sort = counts.sortByKey();
		System.out.println("----------next sort----------------------");
		/**
		 * collect方法其实之前已经出现了多次，该方法用于将spark的RDD类型转化为我们熟知的java常见类型
		 */
		// List<Tuple2<String, Integer>> output = sort.collect();
		// for (Tuple2<?,?> tuple : output) {
		// System.out.println(tuple._1 + ": " + tuple._2());
		// }
		/**
		 * 保存函数，数据输出，spark为结果输出提供了很多接口
		 */
		// sort.saveAsTextFile("/tmp/spark-tmp/test");
		// sort.saveAsNewAPIHadoopFile();
		// sort.saveAsHadoopFile();
		System.exit(0);
	}
	
	public static void test2(){
		String master = "local";// 访问方式: local | yarn-client | yarn-standalone
		String appName = "SparkML";//应用名称
		String spark_home = System.getenv("SPARK_HOME");
		System.out.println(spark_home);
		System.out.println("---------------------------------------------------");
		String[] jars = JavaSparkContext.jarOfClass(SparkTest.class);
		for (String jar : jars) {
			System.out.println(jar);
		}
		System.out.println("---------------------------------------------------");
		
		JavaSparkContext sc = new JavaSparkContext(master, appName);
		List<String> data1 = new ArrayList<String>();
		data1.add("11,22,33,44,55");
		data1.add("aa,bb,cc,dd,ee");
		JavaRDD<String> list = sc.parallelize(data1);
		JavaRDD<String[]> data_map = list.map(new Function<String, String[]>() {
			@Override
			public String[] call(String arg){
				return arg.split(",");
			}
		});
		System.out.println("map :"+JSON.toJSONString(data_map.collect()));
		JavaRDD<String> data_flatMap = list.flatMap(new FlatMapFunction<String, String>() {
			@Override
			public Iterator<String> call(String arg){
				return Arrays.asList(arg.split(",")).iterator();
			}
		});
		System.out.println("flatMap :"+JSON.toJSONString(data_flatMap.collect()));
		JavaRDD<String> data_filter = list.filter(new Function<String, Boolean>() {
			@Override
			public Boolean call(String arg){
				return arg.contains("a");
			}
		});
		System.out.println("filter :"+JSON.toJSONString(data_filter.collect()));
		List<String> data2 = new ArrayList<String>();
		data2.add("1,2,3,4,5");
		data2.add("a,b,c,d,e");
		JavaRDD<String> data_union = list.union(sc.parallelize(data2));
		System.out.println("union :"+JSON.toJSONString(data_union.collect()));
		System.out.println("---------------------------------------------------");
//		Tuple2
//		JavaPairRDD<String,List<String>> datap_union = sc.parallelizePairs(list);
		
		
		
	}
	
	public static void test3(){
		long start = System.currentTimeMillis();
		String master = "local[5]";// 主节点运行模式: local | yarn-client | yarn-standalone
		String appName = "SparkML";// 应用名称
		String spark_home = System.getenv("SPARK_HOME");
		JavaSparkContext sc = new JavaSparkContext(master, appName);
		String path = System.getProperty("user.dir") + "/src/main/resources";
		List<String> list = new ArrayList<String>();
		list.add("1,2,3,4,5");
		JavaRDD<String> data = sc.parallelize(list);
		JavaRDD<Double> transform = data.flatMap(new FlatMapFunction<String, String>() {
			private static final long serialVersionUID = -6566974523347221253L;
			@Override
			public Iterator<String> call(String arg) {
				return Arrays.asList(arg.split(",")).iterator();
			}
		}).map(new Function<String, Double>() {
			private static final long serialVersionUID = -3794599272503327032L;
			@Override
			public Double call(String v1) throws Exception {
				return Double.valueOf(v1);
			}
		});
		Map<Double, Long> kdata = transform.zipWithIndex().collectAsMap();
		Map<Double, Long> _kdata = transform.zipWithUniqueId().collectAsMap();
		System.out.println(JSON.toJSONString(kdata));
		System.out.println("------------>>>>>-------------------");
		System.out.println(JSON.toJSONString(_kdata));
		long end = System.currentTimeMillis();
		double time = (end - start) / 1000.00;
		double ss = time % 60;
		int mm = Double.valueOf(time / 60).intValue() % 60;
		int hh = Double.valueOf(time / 60 / 60).intValue() % 60;
		System.out.println("-------------------------test3耗时:"+(hh>0?hh+"小时":"")+(mm>0?mm+"分钟":"")+ss+"秒"+"-------------------------");
		System.exit(0);
	}
	public static void main(String[] args) throws Exception {
		long start = System.currentTimeMillis();
		String path = SparkTest.class.getResource("/").getPath();
		System.out.println(path);
		String path1 = System.getProperty("user.dir")+"/src/main/resources";
		System.out.println(path1);
		System.out.println("======================================================");
//		FileWriter fw = new FileWriter(path1+"/sample_data.txt");
//		int n = 1*1000;
//		for(int i=1;i<=n;i++){
//			if(i%100==1){
//				fw.write(""+i);
//			}else{
//				fw.write(","+i+(i%100==0&&i!=n?"\n":""));
//			}
//		}
//		fw.flush();
//		fw.close();
		test3();
		System.out.println("======================================================");
		
		long end = System.currentTimeMillis();
		double time = (end-start)/1000.00;
		double ss = time%60;
		int mm = Double.valueOf(time/60).intValue()%60;
		int hh = Double.valueOf(time/60/60).intValue()%60;
		System.out.println("-------------------------main耗时:"+(hh>0?hh+"小时":"")+(mm>0?mm+"分钟":"")+ss+"秒"+"-------------------------");
	}

}
