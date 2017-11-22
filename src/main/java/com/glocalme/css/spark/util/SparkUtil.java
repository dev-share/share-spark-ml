package com.glocalme.css.spark.util;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.DoubleFunction;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.classification.ClassificationModel;
import org.apache.spark.mllib.classification.LogisticRegressionModel;
import org.apache.spark.mllib.classification.LogisticRegressionWithSGD;
import org.apache.spark.mllib.classification.NaiveBayes;
import org.apache.spark.mllib.classification.NaiveBayesModel;
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.optimization.SquaredL2Updater;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.regression.LinearRegressionModel;
import org.apache.spark.mllib.regression.LinearRegressionWithSGD;
import org.apache.spark.mllib.tree.DecisionTree;
import org.apache.spark.mllib.tree.configuration.Algo;
import org.apache.spark.mllib.tree.impurity.Impurity;
import org.apache.spark.mllib.tree.model.DecisionTreeModel;
import org.apache.spark.rdd.RDD;
import org.jblas.DoubleMatrix;

import scala.Tuple2;
import scala.Tuple3;

public class SparkUtil {
	/**
	 * @decription 余弦相似度(两个向量的点积与各向量范数（或长度）的乘积的商)
	 * @author yi.zhang
	 * @time 2017年6月27日 下午6:05:36
	 * @param vec1
	 * @param vec2
	 * @return
	 */
	public static double cosineSimilarity(DoubleMatrix vec1,DoubleMatrix vec2){
		return vec1.dot(vec2) / (vec1.norm2() * vec2.norm2());
	}
	/**
	 * @decription K值平均准确率
	 * @author yi.zhang
	 * @time 2017年6月29日 下午4:15:30
	 * @param actual	实际值集合
	 * @param predicted	预测值集合
	 * @param k			前k个数据
	 * @return
	 */
	public static double avgPrecisionK(List<Integer> actual,List<Integer> predicted,int k){
		List<Integer> predK = predicted.subList(0, k);
		double score = 0.0;
		double numHits = 0.0;
		for (int i=0;i<predK.size();i++) {
			Integer p = predK.get(i);
			if (actual.contains(p)) {
				numHits += 1.0;
				score += numHits / (i + 1.0);
			}
		}
		if (actual.isEmpty()) {
			return 1.0;
		} else {
			return score / Math.min(actual.size(), k);
		}
	}
	
	/**
	 * @decription 训练逻辑模型(参数调优)
	 * @author yi.zhang
	 * @time 2017年7月5日 上午9:54:26
	 * @param input			训练数据
	 * @param regParam		
	 * @param numIterations	迭代次数
	 * @param stepSize		步长
	 * @return
	 */
	@SuppressWarnings("deprecation")
	public static LogisticRegressionModel trainLRWithParams(RDD<LabeledPoint> input,double regParam,int numIterations,double stepSize){
		SquaredL2Updater updater = new SquaredL2Updater();
		LogisticRegressionWithSGD lr = new LogisticRegressionWithSGD();
		lr.optimizer().setNumIterations(numIterations).setUpdater(updater).setRegParam(regParam).setStepSize(stepSize);
		LogisticRegressionModel model = lr.run(input);
//		LogisticRegressionWithSGD.train(input, numIterations, stepSize);
		return model;
	}
	/**
	 * @decription 训练树模型(参数调优)
	 * @author yi.zhang
	 * @time 2017年7月5日 上午10:00:25
	 * @param input
	 * @param maxDepth
	 * @param impurity
	 * @return
	 */
	public static DecisionTreeModel trainDTWithParams(RDD<LabeledPoint> input,int maxDepth,Impurity impurity){
		DecisionTreeModel model = DecisionTree.train(input, Algo.Classification(), impurity, maxDepth);
		return model;
	}
	/**
	 * @decription 训练朴素贝叶斯模型(参数调优)
	 * @author yi.zhang
	 * @time 2017年7月5日 上午10:16:07
	 * @param input
	 * @param lambda	相加平滑度(数据中某个特征和某个类别的组合不存在时不是问题)
	 * @return
	 */
	public static NaiveBayesModel trainNBWithParams(RDD<LabeledPoint> input,double lambda){
		NaiveBayes nb = new NaiveBayes();
		nb.setLambda(lambda);
		NaiveBayesModel model = nb.run(input);
		return model;
	}
	/**
	 * @decription 分类模型评估(逻辑模型|SVM模型)
	 * @author yi.zhang
	 * @time 2017年7月5日 上午9:42:44
	 * @param label	标签
	 * @param data	测试数据
	 * @param model	模型
	 * @return
	 */
	public static Tuple3<String,Double,Double> createMetrics(String label,RDD<LabeledPoint> data,final ClassificationModel model){
		JavaRDD<Tuple2<Object,Object>> scoreAndLabels = data.toJavaRDD().map(new Function<LabeledPoint,Tuple2<Object,Object>>(){
			private static final long serialVersionUID = -8706879424322021319L;
			public Tuple2<Object,Object> call(LabeledPoint point) throws Exception {
				double score = model.predict(point.features());
				return new Tuple2<Object,Object>(score,point.label());
			}
		});
		BinaryClassificationMetrics metrics = new BinaryClassificationMetrics(scoreAndLabels.rdd());
		return new Tuple3<String,Double,Double>(label,metrics.areaUnderROC(),metrics.areaUnderPR());
	}
	
	/**
	 * @decription 二元编码
	 * @author yi.zhang
	 * @time 2017年7月5日 下午3:53:57
	 * @param rdd
	 * @param dx
	 * @return
	 */
	public static Map<String, Long> get_mapping(JavaRDD<String> rdd,final int dx){
		Map<String, Long> map = rdd.map(new Function<String, String>() {
			private static final long serialVersionUID = 1L;
			public String call(String arg) throws Exception {
				String[] r = arg.split(",");
				return r[dx];
			}
		}).distinct().zipWithIndex().collectAsMap();
		return map;
	}
	/**
	 * @decription 均方差MSE
	 * @author yi.zhang
	 * @time 2017年7月6日 上午11:21:36
	 * @param actual	实际值
	 * @param pred		预测值
	 * @return
	 */
	public static double squared_error(double actual, double pred){
		return Math.pow(pred - actual,2);
	}
	/**
	 * @decription 平均绝对误差MAE
	 * @author yi.zhang
	 * @time 2017年7月6日 上午11:21:58
	 * @param actual	实际值
	 * @param pred		预测值
	 * @return
	 */
	public static double abs_error(double actual, double pred){
		return Math.abs(pred - actual);
	}
	/**
	 * @decription 均方根对数误差RMSLE
	 * @author yi.zhang
	 * @time 2017年7月6日 上午11:22:19
	 * @param actual	实际值
	 * @param pred		预测值
	 * @return
	 */
	public static double squared_log_error(double actual, double pred){
		return Math.pow(Math.log(pred+1) - Math.log(actual+1),2);
	}
	/**
	 * @decription 逻辑回归参数调优
	 * @author yi.zhang
	 * @time 2017年7月6日 下午3:43:56
	 * @param train		训练数据
	 * @param test		测试数据
	 * @param iterations	迭代次数
	 * @param step		步长
	 * @param regParam	正则参数
	 * @param regType	正则方式
	 * @param intercept	截距
	 * @return
	 */
	public static double evaluate_lr(JavaRDD<LabeledPoint> train,JavaRDD<LabeledPoint> test,int iterations,double step,double regParam,String regType,boolean intercept){
//		LinearRegressionWithSGD lr = new LinearRegressionWithSGD();
//		lr.setIntercept(intercept);//截距
//		Updater updater = new SimpleUpdater();
//		if(regType.equalsIgnoreCase("L2")||regType.equalsIgnoreCase("L1")){
//			updater = regType.equalsIgnoreCase("L1")?new L1Updater():new SquaredL2Updater();
//		}
//		lr.optimizer().setUpdater(updater).setRegParam(regParam);//正则方式及其参数
//		lr.optimizer().setStepSize(step).setNumIterations(iterations);
//		LinearRegressionModel _model = lr.train(train.rdd(), iterations, step, 1.0);
		final LinearRegressionModel model = LinearRegressionWithSGD.train(train.rdd(), iterations, step, 1.0);
		JavaRDD<Tuple2<Double, Double>> tp = test.map(new Function<LabeledPoint,Tuple2<Double,Double>>(){
			private static final long serialVersionUID = 2164755036956258861L;
			public Tuple2<Double, Double> call(LabeledPoint v1) throws Exception {
				double actual = v1.label();
				double pred = model.predict(v1.features());
				return new Tuple2<Double, Double>(actual,pred);
			}
		});
		double rmsle = Math.sqrt(tp.mapToDouble(new DoubleFunction<Tuple2<Double,Double>>(){
			private static final long serialVersionUID = 1909735037664834392L;
			public double call(Tuple2<Double, Double> t) throws Exception {
				double actual = t._1();
				double pred = t._2();
				return squared_log_error(actual, pred);
			}
		}).mean());
		return rmsle;
	}
	/**
	 * @decription 决策树模型参数调优
	 * @author yi.zhang
	 * @time 2017年7月6日 下午4:52:24
	 * @param train		训练数据
	 * @param test		测试数据
	 * @param maxDepth	最大的树深度
	 * @param maxBins	最大划分数
	 * @return
	 */
	public static double evaluate_dt(JavaRDD<LabeledPoint> train,JavaRDD<LabeledPoint> test,int maxDepth,int maxBins){
		DecisionTreeModel model = DecisionTree.trainRegressor(train, new HashMap<Integer,Integer>(), "variance", maxDepth, maxBins);
		JavaRDD<Double> preds = model.predict(test.map(new Function<LabeledPoint,Vector>(){
			private static final long serialVersionUID = 7388800232842705216L;
			public Vector call(LabeledPoint v1) throws Exception {
				return v1.features();
			}
		}));
		JavaRDD<Double> actual = test.map(new Function<LabeledPoint,Double>(){
			private static final long serialVersionUID = -5768569081281096187L;
			public Double call(LabeledPoint v1) throws Exception {
				return v1.label();
			}
		});
		JavaPairRDD<Double, Double>	tp = actual.zip(preds);
		double rmsle = Math.sqrt(tp.mapToDouble(new DoubleFunction<Tuple2<Double,Double>>(){
			private static final long serialVersionUID = -8908824130626102171L;
			public double call(Tuple2<Double, Double> t) throws Exception {
				double actual = t._1();
				double pred = t._2();
				return squared_log_error(actual, pred);
			}
		}).mean());
		return rmsle;
	}
	
	/**
	 * @decription 文本分词过滤(过滤[数字|停用词|低频字符串])
	 * @author yi.zhang
	 * @time 2017年7月12日 下午4:39:36
	 * @param text		目标词
	 * @return
	 */
	public static JavaRDD<String> tokenize(JavaRDD<String> text){
		JavaPairRDD<String, Long> tokenCounts = text.flatMap(new FlatMapFunction<String, String>(){
			private static final long serialVersionUID = 8028027043186306937L;
			public Iterator<String> call(String t) throws Exception {
				return Arrays.asList(t.split("(\\W+)")).iterator();//过滤符号
			}
		}).map(new Function<String,String>(){
			private static final long serialVersionUID = 7305963887313075084L;
			public String call(String v1) throws Exception {
				return v1.toLowerCase();
			}
		}).filter(new Function<String, Boolean>(){
			private static final long serialVersionUID = 7605510039216974581L;
			public Boolean call(String v1) throws Exception {
				return v1.matches("([^0-9]*)");//过滤数字
			}
		}).mapToPair(new PairFunction<String, String, Long>() {
			private static final long serialVersionUID = 8623062348882325774L;
			public Tuple2<String, Long> call(String n) throws Exception {
				return new Tuple2<String, Long>(n, 1l);
			}
		}).reduceByKey(new Function2<Long, Long, Long>(){
			private static final long serialVersionUID = -8543095048185772337L;
			public Long call(Long n1, Long n2) throws Exception {
				return n1+n2;
			}
		});//词组统计
		//从词组统计中获取低频词
		final List<String> rareTokens = tokenCounts.filter(new Function<Tuple2<String,Long>, Boolean>(){
			private static final long serialVersionUID = 2752957636251643182L;
			public Boolean call(Tuple2<String,Long> v) throws Exception {
				return v._2()<2;
			}
		}).map(new Function<Tuple2<String,Long>, String>(){
			private static final long serialVersionUID = -4339831411083874013L;
			public String call(Tuple2<String, Long> v1) throws Exception {
				return v1._1();
			}
		}).collect();
		
		final HashSet<String> stopwords = new HashSet<String>(Arrays.asList(new String[]{"the","a","an","of","or","in","for","by","on","but", "is", "not","with", "as", "was", "if","they", "are", "this", "and", "it", "have", "from", "at", "my","be", "that", "to"}));
		JavaRDD<String> tokenFilter = tokenCounts.filter(new Function<Tuple2<String,Long>, Boolean>(){
			private static final long serialVersionUID = -4633359331982031363L;
			public Boolean call(Tuple2<String,Long> v) throws Exception {
				return !stopwords.contains(v._1());//过滤停用词
			}
		}).filter(new Function<Tuple2<String,Long>, Boolean>(){
			private static final long serialVersionUID = -277762926206157798L;
			public Boolean call(Tuple2<String,Long> v) throws Exception {
				return v._1().length()>=2;//过滤低频词
			}
		}).filter(new Function<Tuple2<String,Long>, Boolean>(){
			private static final long serialVersionUID = 2569014326714526208L;
			public Boolean call(Tuple2<String,Long> v) throws Exception {
				return !rareTokens.contains(v._1());//过滤低频词
			}
		}).map(new Function<Tuple2<String,Long>, String>() {
			private static final long serialVersionUID = 65429930914004853L;
			public String call(Tuple2<String, Long> v1) throws Exception {
				return v1._1();
			}
		});
		return tokenFilter;
	}
}
