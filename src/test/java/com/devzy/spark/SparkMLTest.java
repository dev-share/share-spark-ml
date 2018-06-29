package com.ucloudlink.spark;

import java.awt.Graphics;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.PrintWriter;
import java.io.Serializable;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import javax.imageio.ImageIO;

import org.apache.commons.collections.IteratorUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.DoubleFunction;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.mllib.classification.ClassificationModel;
import org.apache.spark.mllib.classification.LogisticRegressionModel;
import org.apache.spark.mllib.classification.LogisticRegressionWithSGD;
import org.apache.spark.mllib.classification.NaiveBayes;
import org.apache.spark.mllib.classification.NaiveBayesModel;
import org.apache.spark.mllib.classification.SVMModel;
import org.apache.spark.mllib.classification.SVMWithSGD;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics;
import org.apache.spark.mllib.evaluation.MulticlassMetrics;
import org.apache.spark.mllib.evaluation.RankingMetrics;
import org.apache.spark.mllib.evaluation.RegressionMetrics;
import org.apache.spark.mllib.feature.HashingTF;
import org.apache.spark.mllib.feature.IDF;
import org.apache.spark.mllib.feature.IDFModel;
import org.apache.spark.mllib.feature.StandardScaler;
import org.apache.spark.mllib.feature.StandardScalerModel;
import org.apache.spark.mllib.feature.Word2Vec;
import org.apache.spark.mllib.feature.Word2VecModel;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.linalg.SingularValueDecomposition;
import org.apache.spark.mllib.linalg.SparseVector;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.linalg.distributed.RowMatrix;
import org.apache.spark.mllib.optimization.SquaredL2Updater;
import org.apache.spark.mllib.recommendation.ALS;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.recommendation.Rating;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.regression.LinearRegressionModel;
import org.apache.spark.mllib.regression.LinearRegressionWithSGD;
import org.apache.spark.mllib.regression.StreamingLinearRegressionWithSGD;
import org.apache.spark.mllib.stat.MultivariateStatisticalSummary;
import org.apache.spark.mllib.tree.DecisionTree;
import org.apache.spark.mllib.tree.configuration.Algo;
import org.apache.spark.mllib.tree.impurity.Entropy;
import org.apache.spark.mllib.tree.impurity.Impurity;
import org.apache.spark.mllib.tree.model.DecisionTreeModel;
import org.apache.spark.rdd.RDD;
import org.apache.spark.serializer.GenericAvroSerializer;
import org.apache.spark.serializer.JavaSerializer;
import org.apache.spark.serializer.KryoSerializer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.bson.Document;
import org.jblas.DoubleMatrix;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.datastax.spark.connector.japi.CassandraJavaUtil;
import com.glocalme.share.spark.common.SparkComparator;
import com.glocalme.share.spark.util.DateUtil;
import com.glocalme.share.spark.util.StringUtil;
import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.ReadConfig;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;
import com.ucloudlink.spark.pojo.classify.Train;
import com.ucloudlink.spark.pojo.classify.Train.Boilerplate;
import com.ucloudlink.spark.pojo.cluster.Consumer;
import com.ucloudlink.spark.pojo.cluster.Film;
import com.ucloudlink.spark.pojo.cluster.Genre;
import com.ucloudlink.spark.pojo.cluster.Grade;
import com.ucloudlink.spark.pojo.recommend.Level;
import com.ucloudlink.spark.pojo.recommend.Movie;
import com.ucloudlink.spark.pojo.recommend.User;
import com.ucloudlink.spark.pojo.regression.BikeDay;
import com.ucloudlink.spark.pojo.regression.BikeHour;

import breeze.linalg.DenseVector;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple5;
import scala.util.Random;
//@SuppressWarnings("all")
public class SparkMLTest implements Serializable{
	private static final long serialVersionUID = -5837985289070692632L;
	/**
	 * @decription K之1编码(分词处理)
	 * @author yi.zhang
	 * @time 2017年6月27日 下午3:10:56
	 */
	public static void k_1(){
		long start = System.currentTimeMillis();
		String master = "local[5]";// 主节点运行模式: local | yarn-client | yarn-standalone
		String appName = "SparkML";// 应用名称
		String spark_home = System.getenv("SPARK_HOME");
		System.out.println("-------------------------------------------------------------------"+spark_home);
		JavaSparkContext sc = new JavaSparkContext(master, appName);
		sc.getConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
		sc.getConf().registerKryoClasses(new Class[] { JavaSerializer.class,KryoSerializer.class,GenericAvroSerializer.class });
		String path = System.getProperty("user.dir") + "/src/main/resources"+"/data/ml-100k";
		JavaRDD<String> movie_data = sc.textFile(path + "/u.item");
		JavaRDD<Movie> movies = movie_data.map(new Function<String, Movie>() {
			private static final long serialVersionUID = -6700419064378704418L;
			@Override
			public Movie call(String arg) {
				String[] datas = arg.split("\\|");
				Movie movie = new Movie();
				movie.setId(Integer.valueOf(datas[0]));
				movie.setTitle(datas.length>1?datas[1]:null);
				movie.setDate(datas.length>2?DateUtil.formatDateTime(datas[2], "dd-MMM-yyyy"):null);
				movie.setUrl(datas.length>4?datas[4]:null);
				return movie;
			}
		});
		JavaRDD<String> words =  movies.map(new Function<Movie, String>() {
			private static final long serialVersionUID = -9209661675280070667L;
			public String call(Movie movie) {return movie.getTitle();}
		}).map(new Function<String, String>() {
			private static final long serialVersionUID = 7077107709772222538L;
			public String call(String arg) {return arg.replaceAll("\\((\\w+)\\)", "");}
		}).flatMap(new FlatMapFunction<String, String>() {
			private static final long serialVersionUID = -4376659586875000515L;
			public Iterator<String> call(String arg) {return Arrays.asList(arg.split(" ")).iterator();}
		}).distinct();
		List<String> cwords = words.collect();
		System.out.println(JSON.toJSONString(cwords));
		System.out.println("----------------------------");
//		Collections.sort(cwords,Collator.getInstance(java.util.Locale.CHINA));
//		Map<String,Long> w_k_1 = new HashMap<String, Long>();
//		for (int i=0;i<cwords.size();i++) {
//			String key = cwords.get(i);
//			long value = i;
//			w_k_1.put(key, value);
//		}
//		System.out.println("-1---Dead:"+w_k_1.get("Dead")+",Rooms:"+w_k_1.get("Rooms"));
		Map<String,Long> n_k_1 = words.zipWithIndex().collectAsMap();
		System.out.println("-2---Dead:"+n_k_1.get("Dead")+",Rooms:"+n_k_1.get("Rooms"));
		long end = System.currentTimeMillis();
		double time = (end - start) / 1000.00;
		double ss = time % 60;
		int mm = Double.valueOf(time / 60).intValue() % 60;
		int hh = Double.valueOf(time / 60 / 60).intValue() % 60;
		System.out.println("-------------------------test3耗时:"+(hh>0?hh+"小时":"")+(mm>0?mm+"分钟":"")+ss+"秒"+"-------------------------");
		sc.close();
		System.exit(1);
	}
	/**
	 * @decription 推荐训练模型
	 * @author yi.zhang
	 * @time 2017年6月27日 下午3:10:56
	 */
	public void recommend(){
		long start = System.currentTimeMillis();
		String master = "local[*]";// 主节点运行模式: local | yarn-client | yarn-standalone
		String appName = "SparkMLTest";// 应用名称
		String spark_home = System.getenv("SPARK_HOME");
		System.out.println("-------------------------------------------------------------------"+spark_home);
		SparkConf conf = new SparkConf();
		conf.setMaster(master);
		conf.setAppName(appName);
		conf.setSparkHome(spark_home);
		conf.set("spark.driver.allowMultipleContexts", "true");
		conf.registerKryoClasses(new Class[] { JavaSerializer.class,KryoSerializer.class,GenericAvroSerializer.class });  
//		JavaSparkContext sc = new JavaSparkContext(master, appName);
		final JavaSparkContext sc = new JavaSparkContext(conf);
		String path = System.getProperty("user.dir") + "/src/main/resources"+"/data/ml-100k";
		JavaRDD<String> user_data = sc.textFile(path + "/u.user");
		JavaRDD<User> users = user_data.map(new Function<String, User>() {
			private static final long serialVersionUID = 7986105563887809864L;
			@Override
			public User call(String arg) {
				String[] datas = arg.split("\\|");
				User user = new User();
				user.setId(Integer.valueOf(datas[0]));
				user.setAge(datas.length>1?Integer.valueOf(datas[1]):0);
				user.setGender(datas.length>2?Character.valueOf(datas[2].charAt(0)):0);
				user.setDuty(datas.length>3?datas[3]:null);
				user.setCode(datas.length>4?datas[4]:null);
				return user;
			}
		});
		Map<Integer,User> user_map = new HashMap<Integer,User>();
		for(User user:users.collect()){
			user_map.put(user.getId(), user);
		}
		JavaRDD<String> movie_data = sc.textFile(path + "/u.item");
		JavaRDD<Movie> movies = movie_data.map(new Function<String, Movie>() {
			private static final long serialVersionUID = -6778694340933856280L;
			@Override
			public Movie call(String arg) {
				String[] datas = arg.split("\\|");
				Movie movie = new Movie();
				movie.setId(Integer.valueOf(datas[0]));
				movie.setTitle(datas.length>1?datas[1]:null);
				movie.setDate(datas.length>2?DateUtil.formatDateTime(datas[2], "dd-MMM-yyyy"):null);
				movie.setUrl(datas.length>4?datas[4]:null);
				return movie;
			}
		});
		Map<Integer,Movie> movie_map = new HashMap<Integer,Movie>();
		for(Movie movie:movies.collect()){
			movie_map.put(movie.getId(), movie);
		}
		JavaRDD<String> level_data = sc.textFile(path + "/u.data");
		JavaRDD<Level> levels = level_data.map(new Function<String, Level>() {
			private static final long serialVersionUID = 7528118503210315341L;
			@Override
			public Level call(String arg) {
				String[] datas = arg.split("\t");
				Level level = new Level();
				level.setUserId(Integer.valueOf(datas[0]));
				level.setMovieId(datas.length>1?Integer.valueOf(datas[1]):0);
				level.setScore(datas.length>2?Integer.valueOf(datas[2]):0);
				level.setCreateTime(datas.length>3?new Date(Long.valueOf(datas[3])):null);
				return level;
			}
		});
		Map<String,Double> level_map = new HashMap<String,Double>();
		for(Level level:levels.collect()){
			level_map.put(level.getUserId()+"_"+level.getMovieId(), level.getScore());
		}
		//训练数据
		JavaRDD<Rating> trains = levels.map(new Function<Level, Rating>() {
			private static final long serialVersionUID = 3479561725885413129L;
			public Rating call(Level obj) {
				return new Rating(obj.getUserId(), obj.getMovieId(), obj.getScore());
			}
		});
		int rank = 50;//因子个数(10-200)训练效果与系统开销之间的调节参数
		int iterations = 10;//迭代次数(10次左右最好)降低模型误差
		double lambda = 0.01;//正则参数(与实际数据的大小、特征和稀疏程度有关)
		//标准模型
		MatrixFactorizationModel model = ALS.train(trains.rdd(), rank, iterations,lambda);
		double alpha=1.0;//权重基准线(越高越具备无关性)
		//隐式模型
		MatrixFactorizationModel _model = ALS.trainImplicit(trains.rdd(), rank, iterations,lambda,alpha);
//		System.out.println("User Features :" + JSON.toJSONString(model.userFeatures().collect()));
//		System.out.println("Movie Features :" + JSON.toJSONString(model.productFeatures().collect()));
//		System.out.println("User-Movie Features :" + model.userFeatures().count()+","+model.productFeatures().count());
		int userId = 789;
		int movieId = 123;
		double score = model.predict(userId, movieId);//指定用户对指定商品的预计得分
		System.out.println("user:"+user_map.get(userId).getDuty()+",movie:"+movie_map.get(movieId).getTitle()+",score:"+level_map.get(userId+"_"+movieId)+",level:"+score);
		System.out.println("------------------------------------------------推荐模式[start]------------------------------------------");
		int K = 10;
		Rating[]  recommends = _model.recommendProducts(userId, K);//推荐K个物品
		for (Rating rating : recommends) {
			System.out.println("movie:【"+movie_map.get(rating.product()).getTitle()+"】,level:"+rating.rating());
		}
		System.out.println("---------------check---------------");
		List<Rating> moviesForUser = trains.keyBy(new Function<Rating, Integer>() {
			private static final long serialVersionUID = -3814935754944353625L;
			public Integer call(Rating obj) {return obj.user();}
		}).lookup(789);
		moviesForUser.stream().sorted((r1,r2)->Double.valueOf(r2.rating()-r1.rating()).intValue()).limit(K).forEach(obj -> {
			System.out.println("movie:【"+movie_map.get(obj.product()).getTitle()+"】,level:"+obj.rating());
		});
		System.out.println("------------------------------------");
//		sc.parallelize(moviesForUser).sortBy(new Function<Rating, Double>() {
//			private static final long serialVersionUID = 5475786111395268912L;
//			public Double call(Rating obj) {return obj.rating();}
//		}, false, 1).take(K).forEach(obj -> {
//			System.out.println("movie:【"+movie_map.get(obj.product()).getTitle()+"】,level:"+obj.rating());
//		});
		System.out.println("------------------------------------------------推荐模式[end]------------------------------------------");
		System.out.println("------------------------------------------------余弦相似度[start]------------------------------------------");
		int _movieId = 567;
		DoubleMatrix itemVector = new DoubleMatrix(model.productFeatures().toJavaRDD().filter(new Function<Tuple2<Object,double[]>, Boolean>() {
			private static final long serialVersionUID = -8396573188904472952L;
			@Override
			public Boolean call(Tuple2<Object, double[]> v1) throws Exception {
				return (int)v1._1()==_movieId;
			}
		}).first()._2());
		double sim = cosineSimilarity(itemVector, itemVector);
		System.out.println("sim:"+sim);
		JavaPairRDD<Object, Double> sims = model.productFeatures().toJavaRDD().mapToPair(new PairFunction<Tuple2<Object, double[]>, Object,Double>(){
			private static final long serialVersionUID = -5729690811972794343L;
			@Override
			public Tuple2<Object, Double> call(Tuple2<Object, double[]> v1) throws Exception {
				DoubleMatrix factorVector = new DoubleMatrix(v1._2());
				double sim = cosineSimilarity(factorVector, itemVector);
				return new Tuple2<Object, Double>(v1._1(),sim);
			}
		});
		System.out.println("sims:"+JSON.toJSONString(sims.collectAsMap()));
		List<Tuple2<Object, Double>> sortedSims =sims.top(K, new SparkComparator<Tuple2<Object, Double>>(){
			private static final long serialVersionUID = 1061486410182225076L;
			@Override
			public int compare(Tuple2<Object, Double> o1, Tuple2<Object, Double> o2) {
				return Double.valueOf(o1._2()-o2._2()).intValue();
			}
		});
		System.out.println(JSON.toJSONString(sortedSims));
		System.out.println("------------------------------------------------余弦相似度[end]------------------------------------------");
		System.out.println("------------------------------------------------均方差[start]------------------------------------------");
		
//		Rating actualRating = sc.parallelize(moviesForUser).sortBy(new Function<Rating, Double>() {public Double call(Rating obj) {return obj.rating();}}, false, 1).take(10).get(0);
//		double predictedRating = model.predict(789, actualRating.product());
//		double squaredError = Math.pow(predictedRating - actualRating.rating(), 2.0);
//		System.out.println(actualRating.rating()+","+predictedRating+","+squaredError);
		JavaPairRDD<Integer,Integer> usersProducts = trains.mapToPair(new PairFunction<Rating, Integer,Integer>() {
			private static final long serialVersionUID = -8440146839708715071L;
			public Tuple2<Integer, Integer> call(Rating obj) {
				return new Tuple2<Integer, Integer>(obj.user(), obj.product());
			}
		});
		//预测评级
		JavaPairRDD<Tuple2<Integer, Integer>, Double> predictions =  model.predict(usersProducts).mapToPair(new PairFunction<Rating, Tuple2<Integer, Integer>,Double>() {
			private static final long serialVersionUID = 2036974812569910381L;
			public Tuple2<Tuple2<Integer, Integer>, Double> call(Rating obj) {
				return new Tuple2<Tuple2<Integer, Integer>, Double>(new Tuple2<Integer, Integer>(obj.user(), obj.product()), obj.rating());
			}
		});
		//实际评级
		JavaPairRDD<Tuple2<Integer, Integer>, Double> actuals =  trains.mapToPair(new PairFunction<Rating, Tuple2<Integer, Integer>,Double>() {
			private static final long serialVersionUID = -5175746804426068071L;
			public Tuple2<Tuple2<Integer, Integer>, Double> call(Rating obj) {
				return new Tuple2<Tuple2<Integer, Integer>, Double>(new Tuple2<Integer, Integer>(obj.user(), obj.product()), obj.rating());
			}
		});
		JavaPairRDD<Tuple2<Integer, Integer>, Tuple2<Double, Double>> ratingsAndPredictions =actuals.join(predictions);
		//平方差之和
		double ses = ratingsAndPredictions.map(new Function<Tuple2<Tuple2<Integer, Integer>, Tuple2<Double, Double>>,Double>(){
			private static final long serialVersionUID = 866973873588230530L;
			@Override
			public Double call(Tuple2<Tuple2<Integer, Integer>, Tuple2<Double, Double>> v1) throws Exception {
				double actual=v1._2()._1();
				double predicted=v1._2()._2();
				return Math.pow((actual - predicted), 2);
			}
		}).reduce(new Function2<Double, Double, Double>(){
			private static final long serialVersionUID = -9202271782302099017L;
			@Override
			public Double call(Double v1, Double v2) throws Exception {
				return v1+v2;
			}
		});
		//均方差
		double mse = ses/ratingsAndPredictions.count();
		//均方根误差
		double rmse = Math.sqrt(mse);
		System.out.println("MSE:"+mse+",RMSE:"+rmse);
		System.out.println("------------------------------------------------均方差[end]------------------------------------------");
		System.out.println("------------------------------------------------K值平均准确率[start]------------------------------------------");
		JavaRDD<double[]> factors = model.productFeatures().toJavaRDD().map(new Function<Tuple2<Object, double[]>, double[]>(){
			private static final long serialVersionUID = 5016713921689231294L;
			@Override
			public double[] call(Tuple2<Object, double[]> t) throws Exception {
				double[] scores = t._2();
				return scores;
			}
		});
		List<double[]> itemFactors = factors.collect();
		double[][] data = new double[itemFactors.size()][];
		itemFactors.toArray(data);
		DoubleMatrix itemMatrix = new DoubleMatrix(data);
		System.out.println("itemMatrix:"+itemMatrix.rows+","+itemMatrix.columns);
		Broadcast<DoubleMatrix> imBroadcast = sc.broadcast(itemMatrix);
		JavaPairRDD<Integer, Iterable<Integer>> allRecs = model.userFeatures().toJavaRDD().mapToPair(new PairFunction<Tuple2<Object, double[]>, Integer,Iterable<Integer>>(){
			private static final long serialVersionUID = -5729690811972794343L;
			@Override
			public Tuple2<Integer, Iterable<Integer>> call(Tuple2<Object, double[]> v1) throws Exception {
				DoubleMatrix userVector = new DoubleMatrix(v1._2());//用户因子矩阵
				DoubleMatrix scores = imBroadcast.getValue().mmul(userVector);//电影因子矩阵*用户因子矩阵
				double[] data = scores.data;//电影预测评级
				Double[] _data = new Double[data.length];
				for (int i=0;i<data.length;i++) {
					_data[i] = data[i];
				}
				Arrays.asList(_data).sort(new Comparator<Double>() {
					@Override
					public int compare(Double o1, Double o2) {
						double diff=o1-o2;
						return diff==0?0:(diff>0?1:-1);
					}
				});
				List<Tuple2<Double,Integer>> sortedWithId=new ArrayList<Tuple2<Double,Integer>>();
				for(int i=0;i<_data.length;i++){//K之1编码
					sortedWithId.add(new Tuple2<Double,Integer>(_data[i],i));
				}
				Stream<Integer> recommendeds = sortedWithId.stream().map(obj->obj._2()+1);//使用得分代替电影ID
				@SuppressWarnings("unchecked")//推荐电影ID
				Iterable<Integer> recommendedIds = IteratorUtils.toList(recommendeds.iterator());
				return new Tuple2<Integer, Iterable<Integer>>((Integer) v1._1(),recommendedIds);
			}
		});
		System.out.println("---allrecs:"+JSON.toJSONString(allRecs.collectAsMap()));
		
		JavaPairRDD<Integer, Iterable<Tuple2<Integer, Integer>>>  userMovies = trains.mapToPair(new PairFunction<Rating, Integer,Integer>() {
			private static final long serialVersionUID = 7145626417439940776L;
			public Tuple2<Integer, Integer> call(Rating obj) {
				return new Tuple2<Integer, Integer>(obj.user(), obj.product());
			}
		}).groupBy(new Function<Tuple2<Integer, Integer>,Integer>(){
			private static final long serialVersionUID = -4714756705570567394L;
			@Override
			public Integer call(Tuple2<Integer, Integer> v1) throws Exception {
				// TODO Auto-generated method stub
				return v1._1();
			}
		});
//		System.out.println("userMovies:"+JSON.toJSONString(userMovies.collectAsMap()));
		// 			用户 					预测电影ID数据			实际数据(用户ID-电影ID)
		JavaPairRDD<Integer, Tuple2<Iterable<Integer>, Iterable<Tuple2<Integer, Integer>>>> recs = allRecs.join(userMovies);
		System.out.println("----------------------------------------------->>>>>>>>>>>>>------------------------------------------------------");
		double apk = recs.map(new Function<Tuple2<Integer,Tuple2<Iterable<Integer>,Iterable<Tuple2<Integer,Integer>>>>, Double>() {
			private static final long serialVersionUID = 4300124011726007134L;
			@Override
			public Double call(Tuple2<Integer, Tuple2<Iterable<Integer>, Iterable<Tuple2<Integer, Integer>>>> v1)throws Exception {
//				Integer userId = v1._1();//用户
				Tuple2<Iterable<Integer>, Iterable<Tuple2<Integer, Integer>>> predicted_actual = v1._2();
				Iterable<Integer> predicted_data=predicted_actual._1();//预测数据(电影ID)
				Iterable<Tuple2<Integer, Integer>> actualWithIds = predicted_actual._2();
				List<Integer> predicted = new ArrayList<Integer>();
				predicted_data.forEach(obj->predicted.add(obj));
				List<Integer> actual = new ArrayList<Integer>();
				//实际数据(用户ID-电影ID)
				actualWithIds.forEach(obj->{
					actual.add(obj._2());
				});
				double apk = avgPrecisionK(actual, predicted, K);
				return apk;
			}
		}).reduce(new Function2<Double, Double, Double>() {
			private static final long serialVersionUID = -1106927390361180057L;
			@Override
			public Double call(Double v1, Double v2) throws Exception {
				return v1+v2;
			}
		});
		double mapk = apk/allRecs.count();
		System.out.println("mapk:"+mapk);//前K个平均准确度(越高越接近真实)
		System.out.println("------------------------------------------------K值平均准确率[end]------------------------------------------");
		System.out.println("------------------------------------------------MLlib内置函数[start]------------------------------------------");
		JavaRDD<Tuple2<Object, Object>> predictedAndTrue = ratingsAndPredictions.map(new Function<Tuple2<Tuple2<Integer, Integer>, Tuple2<Double, Double>>,Tuple2<Object, Object>>(){
			private static final long serialVersionUID = 4876431789843577774L;
			@Override
			public Tuple2<Object, Object> call(Tuple2<Tuple2<Integer, Integer>, Tuple2<Double, Double>> v1) throws Exception {
				Double actual=v1._2()._1();
				Double predicted=v1._2()._2();
				return new Tuple2<Object, Object>(actual,predicted);
			}
		});
		//回归矩阵
		RegressionMetrics regressionMetrics = new RegressionMetrics(predictedAndTrue.rdd());
		System.out.println("MSE:"+regressionMetrics.meanSquaredError()+",RMSE:"+regressionMetrics.rootMeanSquaredError());//越低误差越低
		
		JavaPairRDD<Integer, Tuple2<Iterable<Integer>, Iterable<Tuple2<Integer, Integer>>>> mrecs = allRecs.join(userMovies);
		JavaRDD<Tuple2<Object[], Object[]>>  predictedAndTrueForRanking = mrecs.map(new Function<Tuple2<Integer,Tuple2<Iterable<Integer>,Iterable<Tuple2<Integer,Integer>>>>, Tuple2<Object[], Object[]>>() {
			private static final long serialVersionUID = 6887391752421047285L;
			@Override
			public Tuple2<Object[], Object[]> call(Tuple2<Integer, Tuple2<Iterable<Integer>, Iterable<Tuple2<Integer, Integer>>>> v1)throws Exception {
				Tuple2<Iterable<Integer>, Iterable<Tuple2<Integer, Integer>>> predicted_actual = v1._2();
				Iterable<Integer> predicted_data=predicted_actual._1();//预测数据(电影ID)
				Iterable<Tuple2<Integer, Integer>> actualWithIds = predicted_actual._2();
				List<Integer> predicted = new ArrayList<Integer>();
				predicted_data.forEach(obj->predicted.add(obj));
				List<Integer> actual = new ArrayList<Integer>();
				//实际数据(用户ID-电影ID)
				actualWithIds.forEach(obj->{
					actual.add(obj._2());
				});
				return new Tuple2<Object[], Object[]>(predicted.toArray(),actual.toArray());
			}
		});
		RankingMetrics rankingMetrics = new RankingMetrics(predictedAndTrueForRanking.rdd(), predictedAndTrueForRanking.classTag());
		System.out.println("MAP:"+rankingMetrics.meanAveragePrecision());//全局平均准确度(越高越好)
		System.out.println("------------------------------------------------MLlib内置函数[end]------------------------------------------");
		long end = System.currentTimeMillis();
		double time = (end - start) / 1000.00;
		double ss = time % 60;
		int mm = Double.valueOf(time / 60).intValue() % 60;
		int hh = Double.valueOf(time / 60 / 60).intValue() % 60;
		System.out.println("-------------------------test3耗时:"+(hh>0?hh+"小时":"")+(mm>0?mm+"分钟":"")+ss+"秒"+"-------------------------");
		sc.close();
		System.exit(1);
	}
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
	 * @decription 分类模型
	 * @author yi.zhang
	 * @time 2017年6月29日 下午5:10:15
	 */
	public void classify(){
		long start = System.currentTimeMillis();
		String master = "local[2]";// 主节点运行模式: local | yarn-client | yarn-standalone
		String appName = "SparkMLTest";// 应用名称
		String spark_home = System.getenv("SPARK_HOME");
		System.out.println("-------------------------------------------------------------------"+spark_home);
		JavaSparkContext sc = new JavaSparkContext(master, appName);
		sc.setLogLevel("warn");
		sc.getConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
		sc.getConf().registerKryoClasses(new Class[] { JavaSerializer.class,KryoSerializer.class,GenericAvroSerializer.class });
		String path = System.getProperty("user.dir") + "/src/main/resources"+"/data/kaggle";
		JavaRDD<String> records = sc.textFile(path + "/train_noheader.tsv");
		JavaRDD<Train> odata = records.map(new Function<String, Train>() {
			private static final long serialVersionUID = 610469435838263743L;
			@Override
			public Train call(String arg) {
//				System.out.println("["+(i++)+"]data:"+arg);
				arg=arg.replaceAll("\"\"", "##@##").replaceAll("\"", "");
				String[] datas = arg.split("\t");
				Train obj = new Train();
				String url=datas[0];
				int urlid=Integer.valueOf(datas[1]);
				JSONObject json = datas[2]!=null?JSON.parseObject(datas[2].replaceAll("##@##", "\"")):null;
				Boilerplate boilerplate = obj.new Boilerplate();
				boilerplate.setTitle(json==null?null:json.getString("title"));
				boilerplate.setBody(json==null?null:json.getString("body"));
				boilerplate.setUrl(json==null?null:json.getString("url"));
				String alchemy_category=datas[3];
				double alchemy_category_score=(datas[4]==null||datas[4].equals("?"))?0:Double.valueOf(datas[4]);//double ?=0.0
				double avglinksize=Double.valueOf(datas[5]);
				double commonlinkratio_1=Double.valueOf(datas[6]);
				double commonlinkratio_2=Double.valueOf(datas[7]);
				double commonlinkratio_3=Double.valueOf(datas[8]);
				double commonlinkratio_4=Double.valueOf(datas[9]);
				double compression_ratio=Double.valueOf(datas[10]);
				double embed_ratio=Double.valueOf(datas[11]);
				int framebased=Integer.valueOf(datas[12]);
				double frameTagRatio=Double.valueOf(datas[13]);
				int hasDomainLink=Integer.valueOf(datas[14]);
				double html_ratio=Double.valueOf(datas[15]);
				double image_ratio=Double.valueOf(datas[16]);
				int is_news=(datas[17]==null||datas[17].equals("?"))?0:Integer.valueOf(datas[17]);//int ?=0
				int lengthyLinkDomain=Integer.valueOf(datas[18]);
				double linkwordscore=Double.valueOf(datas[19]);
				int news_front_page=(datas[20]==null||datas[20].equals("?"))?0:Integer.valueOf(datas[20]);//int ?=1
				int non_markup_alphanum_characters=Integer.valueOf(datas[21]);
				int numberOfLinks=Integer.valueOf(datas[22]);
				double numwords_in_url=Double.valueOf(datas[23]);
				double parametrizedLinkRatio=Double.valueOf(datas[24]);
				double spelling_errors_ratio=Double.valueOf(datas[25]);
				int label=Integer.valueOf(datas[26]);
				obj.setAlchemy_category(alchemy_category);
				obj.setAlchemy_category_score(alchemy_category_score);
				obj.setAvglinksize(avglinksize);
				obj.setBoilerplate(boilerplate);
				obj.setCommonlinkratio_1(commonlinkratio_1);
				obj.setCommonlinkratio_2(commonlinkratio_2);
				obj.setCommonlinkratio_3(commonlinkratio_3);
				obj.setCommonlinkratio_4(commonlinkratio_4);
				obj.setCompression_ratio(compression_ratio);
				obj.setEmbed_ratio(embed_ratio);
				obj.setFramebased(framebased);
				obj.setFrameTagRatio(frameTagRatio);
				obj.setHasDomainLink(hasDomainLink);
				obj.setHtml_ratio(html_ratio);
				obj.setImage_ratio(image_ratio);
				obj.setIs_news(is_news);
				obj.setIs_news(is_news);
				obj.setLengthyLinkDomain(lengthyLinkDomain);
				obj.setLinkwordscore(linkwordscore);
				obj.setNews_front_page(news_front_page);
				obj.setNon_markup_alphanum_characters(non_markup_alphanum_characters);
				obj.setNumberOfLinks(numberOfLinks);
				obj.setNumwords_in_url(numwords_in_url);
				obj.setParametrizedLinkRatio(parametrizedLinkRatio);
				obj.setSpelling_errors_ratio(spelling_errors_ratio);
				obj.setUrl(url);
				obj.setUrlid(urlid);
				obj.setLabel(label);
				return obj;
			}
		});
		System.out.println("odata count:"+odata.count());
		JavaRDD<LabeledPoint> data = records.map(new Function<String, LabeledPoint>() {
			private static final long serialVersionUID = -629252815965274599L;
			@Override
			public LabeledPoint call(String arg) {
				String[] r = arg.replaceAll("\"", "").split("\t");
				double[] features = new double[r.length-4];
				for(int i=4;i<r.length;i++){
					features[i-4]=r[i].equals("?")?0.0:Double.valueOf(r[i]);
				}
				Vectors.dense(features);
				int label = Integer.valueOf(r[(r.length - 1)]);
				return new LabeledPoint(label, Vectors.dense(features));
			}
		});
		data.cache();
		long numData = data.count();
		System.out.println("numData:"+numData);
//		List<LabeledPoint> points = data.collect();
//		System.out.println(JSON.toJSONString(points));
		JavaRDD<LabeledPoint> nbData = records.map(new Function<String, LabeledPoint>() {
			private static final long serialVersionUID = -629252815965274599L;
			@Override
			public LabeledPoint call(String arg) {
				String[] r = arg.replaceAll("\"", "").split("\t");
				double[] features = new double[r.length-1-4];
				for(int i=4;i<r.length-1;i++){
					features[i-4]=r[i].equals("?")||r[i].startsWith("-")?0.0:Double.valueOf(r[i]);
				}
				int label = Integer.valueOf(r[(r.length - 1)]);
				return new LabeledPoint(label, Vectors.dense(features));
			}
		});
		long nbnumData = nbData.count();
		System.out.println("nbnumData:"+nbnumData);
		Map<String,Long> categories = records.map(new Function<String,String>(){
			private static final long serialVersionUID = -1712066467752316665L;
			@Override
			public String call(String arg) throws Exception {
				String[] r = arg.replaceAll("\"", "").split("\t");
				return r[3];
			}
		}).distinct().zipWithIndex().collectAsMap();
		System.out.println(categories);
		int numCategories = categories.size();
		System.out.println(numCategories);
		JavaRDD<LabeledPoint> dataCategories = records.map(new Function<String, LabeledPoint>() {
			private static final long serialVersionUID = -629252815965274599L;
			@Override
			public LabeledPoint call(String arg) {
				String[] r = arg.replaceAll("\"", "").split("\t");
				Long categoryIdx = categories.get(r[3]);
				double[] features = new double[numCategories+r.length-1-4];
				features[categoryIdx.intValue()] = 1.0;
				for(int i=4;i<r.length-1;i++){
					features[numCategories+i-4]=r[i].equals("?")||r[i].startsWith("-")?0.0:Double.valueOf(r[i]);
				}
				int label = Integer.valueOf(r[(r.length - 1)]);
				return new LabeledPoint(label, Vectors.dense(features));
			}
		});
		System.out.println(dataCategories.first());
		System.out.println("------------------------------------------------分类模型[start]------------------------------------------------");
		System.out.println("================================================模型预测[1s]================================================");
		int numIterations = 10;//逻辑回归、向量机(SVM)迭代次数
		int maxTreeDepth = 5;//决策树深度
		LogisticRegressionModel lrModel = LogisticRegressionWithSGD.train(data.rdd(), numIterations);//逻辑回归模型
		SVMModel svmModel = SVMWithSGD.train(data.rdd(), numIterations);//向量机(SVM)模型
		NaiveBayesModel nbModel = NaiveBayes.train(nbData.rdd());//朴素贝叶斯模型
		DecisionTreeModel dtModel = DecisionTree.train(data.rdd(), Algo.Classification(), Entropy.instance(), maxTreeDepth);//决策树模型(Algo为算法[Classification,Regression],Impurity为不纯度估计[Entropy分类,Gini分类,Variance回归])
		LabeledPoint points = data.first();
		LabeledPoint _points = nbData.first();
		double plabel = lrModel.predict(points.features());
		System.out.println("lr_label:"+plabel+"-->"+points.label());
		plabel = svmModel.predict(points.features());
		System.out.println("svm_label:"+plabel+"-->"+points.label());
		plabel = nbModel.predict(_points.features());
		System.out.println("nb_label:"+plabel+"-->"+_points.label());
		plabel = dtModel.predict(points.features());
		System.out.println("dt_label:"+plabel+"-->"+points.label());
		JavaRDD<Vector> features = data.map(new Function<LabeledPoint, Vector>() {
			private static final long serialVersionUID = 7703216185385688885L;
			@Override
			public Vector call(LabeledPoint v1) throws Exception {
				return v1.features();
			}
		});
		JavaRDD<Vector> _features = nbData.map(new Function<LabeledPoint, Vector>() {
			private static final long serialVersionUID = 7703216185385688885L;
			@Override
			public Vector call(LabeledPoint v1) throws Exception {
				return v1.features();
			}
		});
		JavaRDD<Vector> _features_ = dataCategories.map(new Function<LabeledPoint, Vector>() {
			private static final long serialVersionUID = 7703216185385688885L;
			@Override
			public Vector call(LabeledPoint v1) throws Exception {
				return v1.features();
			}
		});
		System.out.println("-----------------------------------------------------------------------------------------------------");
		JavaRDD<Double> predictions = lrModel.predict(features);
		List<Double> pvalue = predictions.take(5);
		System.out.println("lr_take(5)_label:"+JSON.toJSONString(pvalue));
		predictions = svmModel.predict(features);
		pvalue = predictions.take(5);
		System.out.println("svm_take(5)_label:"+JSON.toJSONString(pvalue));
		predictions = nbModel.predict(_features);
		pvalue = predictions.take(5);
		System.out.println("nb_take(5)_label:"+JSON.toJSONString(pvalue));
		predictions = dtModel.predict(features);
		pvalue = predictions.take(5);
		System.out.println("dt_take(5)_label:"+JSON.toJSONString(pvalue));
		System.out.println("================================================模型预测[1e]================================================");
		System.out.println("================================================模型评估[2s]================================================");
		System.out.println("1.预测的正确率和错误率:");
		double total_correct = data.mapToDouble(new DoubleFunction<LabeledPoint>() {
			private static final long serialVersionUID = 6274023764505994534L;
			@Override
			public double call(LabeledPoint t) throws Exception {
				if(lrModel.predict(t.features())==t.label()){
					return 1;
				}
				return 0;
			}
		}).sum();
		double accuracy = total_correct / data.count();
		System.err.println("---lr_accuracy:\t"+accuracy);
		total_correct = data.mapToDouble(new DoubleFunction<LabeledPoint>() {
			private static final long serialVersionUID = 6274023764505994534L;
			@Override
			public double call(LabeledPoint t) throws Exception {
				if(svmModel.predict(t.features())==t.label()){
					return 1;
				}
				return 0;
			}
		}).sum();
		accuracy = total_correct / data.count();
		System.err.println("---svm_accuracy:\t"+accuracy);
		total_correct = nbData.mapToDouble(new DoubleFunction<LabeledPoint>() {
			private static final long serialVersionUID = 6274023764505994534L;
			@Override
			public double call(LabeledPoint t) throws Exception {
				if(nbModel.predict(t.features())==t.label()){
					return 1;
				}
				return 0;
			}
		}).sum();
		accuracy = total_correct / nbData.count();
		System.err.println("---nb_accuracy:\t"+accuracy);
		total_correct = data.mapToDouble(new DoubleFunction<LabeledPoint>() {
			private static final long serialVersionUID = 6274023764505994534L;
			@Override
			public double call(LabeledPoint point) throws Exception {
				double score = dtModel.predict(point.features());
				double predicted = score > 0.5?1:0;//0.5为阈值
				if (predicted == point.label()){
					return 1;
				}
				return 0;
			}
		}).sum();
		accuracy = total_correct / data.count();
		System.err.println("---dt_accuracy:\t"+accuracy);
		System.out.println("2.准确率(评价结果的质量)和召回率(评价结果的完整性):[与阈值相关]");//准确率定义为真阳性的数目除以真阳性和假阳性的总数
		System.out.println("3.(准确率-召回率)PR曲线下方的面积:[与阈值相关]");//与阈值相关
		//真阳性[f(1->1)],1-->0:假阴性[f(1->0)],假阳性[f(0->1)],真阴性[f(0->0)]
		System.out.println("4.ROC曲线:{真阳性率(敏感度):TPR=S(f(1->1))/S(f(1->1)+f(1->0)),假阳性率:FPR=S(f(0->1))/S(f(0->1)+f(0->0))}");
		System.out.println("5.ROC曲线下的面积(AUC:平均值):");
		List<Tuple3<String,Double,Double>> allMetrics = new ArrayList<Tuple3<String,Double,Double>>();
		List<ClassificationModel> models = Arrays.asList(new ClassificationModel[]{lrModel, svmModel,nbModel});
		for (ClassificationModel model : models) {
			JavaRDD<Tuple2<Object,Object>> scoreAndLabels = (model instanceof NaiveBayesModel?nbData:data).map(new Function<LabeledPoint,Tuple2<Object,Object>>(){
				private static final long serialVersionUID = 5213349564961868165L;
				@Override
				public Tuple2<Object,Object> call(LabeledPoint point) throws Exception {
					double score = model.predict(point.features());
					score = model instanceof NaiveBayesModel?(score>0.5?1.0:0):score;
					return new Tuple2<Object,Object>(score,point.label());
				}
			});
			BinaryClassificationMetrics metrics = new BinaryClassificationMetrics(scoreAndLabels.rdd());
			Tuple3<String,Double,Double> value = new Tuple3<String,Double,Double>(model.getClass().getSimpleName(),metrics.areaUnderPR(), metrics.areaUnderROC());
			allMetrics.add(value);
		}
		if(dtModel!=null){
			JavaRDD<Tuple2<Object,Object>> scoreAndLabels = data.map(new Function<LabeledPoint,Tuple2<Object,Object>>(){
				private static final long serialVersionUID = -6450532123842540988L;
				@Override
				public Tuple2<Object,Object> call(LabeledPoint point) throws Exception {
					double score = dtModel.predict(point.features());
					return new Tuple2<Object,Object>(score>0.5?1.0:0,point.label());
				}
			});
			BinaryClassificationMetrics metrics = new BinaryClassificationMetrics(scoreAndLabels.rdd());
			Tuple3<String,Double,Double> value = new Tuple3<String,Double,Double>(dtModel.getClass().getSimpleName(),metrics.areaUnderPR(), metrics.areaUnderROC());
			allMetrics.add(value);
		}
		for (Tuple3<String, Double, Double> tuple3 : allMetrics) {
			System.out.println("["+tuple3._1()+"],PR:"+tuple3._2()+",ROC:"+tuple3._3());
		}
		
		RowMatrix matrix = new RowMatrix(features.rdd());
		MultivariateStatisticalSummary  matrixSummary = matrix.computeColumnSummaryStatistics();
		System.out.println("Col Matrix mean:"+matrixSummary.mean());//列均值
		System.out.println("Col Matrix Min:"+matrixSummary.min());//列最小值
		System.out.println("Col Matrix Max:"+matrixSummary.max());//列最大值
		System.out.println("Col Matrix variance:"+matrixSummary.variance());//列方差
		System.out.println("Col Matrix numNonzeros:"+matrixSummary.numNonzeros());//列非零项数目
		//(x – μ) / sqrt(variance) : 每个特征值减去列的均值，然后除以列的标准差以进行缩放
		StandardScalerModel scaler = new StandardScaler(true, true).fit(features.rdd());//减去均值,标准差缩放
		JavaRDD<LabeledPoint> scaledData = data.map(new Function<LabeledPoint, LabeledPoint>() {
			private static final long serialVersionUID = -7180231421881913005L;
			@Override
			public LabeledPoint call(LabeledPoint lp) throws Exception {
				return new LabeledPoint(lp.label(), scaler.transform(lp.features()));
			}
		});
		System.out.println(data.first().features());
		System.out.println(scaledData.first().features());
		LogisticRegressionModel lrModelScaled = LogisticRegressionWithSGD.train(scaledData.rdd(), numIterations);
		JavaRDD<Tuple2<Double,Double>> lrm_predicted_actual = scaledData.map(new Function<LabeledPoint,Tuple2<Double,Double>>(){
			private static final long serialVersionUID = 9060519750247560640L;
			@Override
			public Tuple2<Double,Double> call(LabeledPoint point) throws Exception {
				double score = lrModelScaled.predict(point.features());
				return new Tuple2<Double,Double>(score,point.label());
			}
		});
		double lrTotalCorrectScaled = lrm_predicted_actual.mapToDouble(new DoubleFunction<Tuple2<Double,Double>>() {
			private static final long serialVersionUID = 3735457196940887663L;
			@Override
			public double call(Tuple2<Double,Double> t) throws Exception {
				if(t._1()==t._2()){
					return 1;
				}
				return 0;
			}
		}).sum();
		double accuracy_scaled = lrTotalCorrectScaled / data.count();
		
		BinaryClassificationMetrics lrMetrisharecaled = new BinaryClassificationMetrics(lrm_predicted_actual.map(v->new Tuple2<Object,Object>(v._1(),v._2())).rdd());
		System.err.println("lr_scaled_model:\t"+lrModelScaled.getClass().getSimpleName());
		System.err.println("lr_scaled_accuracy:\t"+accuracy_scaled * 100+"%");
		System.err.println("lr_scaled_PR:\t"+lrMetrisharecaled.areaUnderPR() * 100+"%");
		System.err.println("lr_scaled_ROC:\t"+lrMetrisharecaled.areaUnderROC() * 100+"%");
		//(x – μ) / sqrt(variance) : 每个特征值减去列的均值，然后除以列的标准差以进行缩放
		StandardScalerModel scalerCats = new StandardScaler(true, true).fit(_features_.rdd());//减去均值,标准差缩放
		JavaRDD<LabeledPoint> scaledDataCats = dataCategories.map(new Function<LabeledPoint, LabeledPoint>() {
			private static final long serialVersionUID = -7180231421881913005L;
			@Override
			public LabeledPoint call(LabeledPoint lp) throws Exception {
				return new LabeledPoint(lp.label(), scalerCats.transform(lp.features()));
			}
		});
		System.out.println(scaledDataCats.first().features());
//		SimpleUpdater updater = new SimpleUpdater();//正则化(当正则化不存在或者非常低时，模型容易过拟合;正则化太高可能导致模型欠拟合)
//		LogisticGradient gradient = new LogisticGradient();
//		GradientDescent optimizer = new GradientDescent(gradient, updater);
//		optimizer.setStepSize(stepSize);
//		optimizer.setNumIterations(numIterations);
//		optimizer.setRegParam(regParam);
//		optimizer.setMiniBatchFraction(miniBatchFraction);
		System.out.println("-----------------------------------------------参数调优----------------------------------------------------");
		RDD<LabeledPoint>[] trainTestSplit = scaledDataCats.rdd().randomSplit(new double[]{0.6,0.4}, 123);
		RDD<LabeledPoint> train = trainTestSplit[0];
		RDD<LabeledPoint> test = trainTestSplit[1];
		List<Tuple3<String, Double, Double>> regResultsTest = new ArrayList<Tuple3<String,Double,Double>>();
		for(double param:new Double[]{0.0, 0.001, 0.0025, 0.005, 0.01}){
			LogisticRegressionModel model = trainLRWithParams(train, param, numIterations,1.0);
			Tuple3<String, Double, Double> value = createMetrics(param+"", test, model);
			regResultsTest.add(value);
		}
		regResultsTest.forEach(f->{
			System.out.println("regParam:"+f._1()+"-L2,ROC:"+f._2()+",PR:"+f._3());
		});
		System.out.println("6.F-Measure:");
		System.out.println("================================================模型评估[2e]================================================");
		System.out.println("------------------------------------------------分类模型[end]------------------------------------------------");
		long end = System.currentTimeMillis();
		double time = (end - start) / 1000.00;
		double ss = time % 60;
		int mm = Double.valueOf(time / 60).intValue() % 60;
		int hh = Double.valueOf(time / 60 / 60).intValue() % 60;
		System.out.println("-------------------------classify耗时:"+(hh>0?hh+"小时":"")+(mm>0?mm+"分钟":"")+ss+"秒"+"-------------------------");
		sc.close();
		System.exit(1);
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
	public static Tuple3<String,Double,Double> createMetrics(String label,RDD<LabeledPoint> data,ClassificationModel model){
		JavaRDD<Tuple2<Object,Object>> scoreAndLabels = data.toJavaRDD().map(new Function<LabeledPoint,Tuple2<Object,Object>>(){
			private static final long serialVersionUID = -8706879424322021319L;
			@Override
			public Tuple2<Object,Object> call(LabeledPoint point) throws Exception {
				double score = model.predict(point.features());
				return new Tuple2<Object,Object>(score,point.label());
			}
		});
		BinaryClassificationMetrics metrics = new BinaryClassificationMetrics(scoreAndLabels.rdd());
		return new Tuple3<String,Double,Double>(label,metrics.areaUnderROC(),metrics.areaUnderPR());
	}
	/**
	 * @decription 回归模型
	 * @author yi.zhang
	 * @time 2017年7月5日 下午2:08:46
	 */
	public void regression(){
		long start = System.currentTimeMillis();
		String master = "local[2]";// 主节点运行模式: local | yarn-client | yarn-standalone
		String appName = "SparkMLTest";// 应用名称
		String spark_home = System.getenv("SPARK_HOME");
		System.out.println("-------------------------------------------------------------------"+spark_home);
		JavaSparkContext sc = new JavaSparkContext(master, appName);
//		sc.setLogLevel("warn");
		sc.getConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
		sc.getConf().registerKryoClasses(new Class[] { JavaSerializer.class,KryoSerializer.class,GenericAvroSerializer.class });
		String path = System.getProperty("user.dir") + "/src/main/resources"+"/data/bike";
		JavaRDD<String> drecords = sc.textFile(path + "/day_noheader.csv");
		JavaRDD<BikeDay> days = drecords.map(new Function<String, BikeDay>() {
			private static final long serialVersionUID = 3418903868616380785L;
			@Override
			public BikeDay call(String arg) {
				String[] datas = arg.split(",");
				BikeDay obj = new BikeDay();
				obj.setInstant(Integer.valueOf(datas[0]));
				obj.setDteday(DateUtil.formatDateTime(datas[1], "yyyy/MM/dd"));
				obj.setSeason(Integer.valueOf(datas[2]));
				obj.setYr(Integer.valueOf(datas[3]));
				obj.setMnth(Integer.valueOf(datas[4]));
				obj.setHoliday(Integer.valueOf(datas[5]));
				obj.setWeekday(Integer.valueOf(datas[6]));
				obj.setWorkingday(Integer.valueOf(datas[7]));
				obj.setWeathersit(Integer.valueOf(datas[8]));
				obj.setTemp(Double.valueOf(datas[9]));
				obj.setAtemp(Double.valueOf(datas[10]));
				obj.setHum(Double.valueOf(datas[11]));
				obj.setWindspeed(Double.valueOf(datas[12]));
				obj.setCasual(Integer.valueOf(datas[13]));
				obj.setRegistered(Integer.valueOf(datas[14]));
				obj.setCnt(Integer.valueOf(datas[15]));
				return obj;
			}
		});
		System.out.println("day count:"+days.count());
		JavaRDD<String[]> _records = drecords.map(new Function<String, String[]>() {
			private static final long serialVersionUID = 4297254421745124107L;
			@Override
			public String[] call(String arg) {
				return arg.split(",");
			}
		});
		System.out.println("days count:"+_records.count());
		JavaRDD<String> hrecords = sc.textFile(path + "/hour_noheader.csv");
		JavaRDD<BikeHour> hours = hrecords.map(new Function<String, BikeHour>() {
			private static final long serialVersionUID = -7791932749000077792L;
			@Override
			public BikeHour call(String arg) {
				String[] datas = arg.split(",");
				BikeHour obj = new BikeHour();
				obj.setInstant(Integer.valueOf(datas[0]));
				obj.setDteday(DateUtil.formatDateTime(datas[1], "yyyy/MM/dd"));
				obj.setSeason(Integer.valueOf(datas[2]));
				obj.setYr(Integer.valueOf(datas[3]));
				obj.setMnth(Integer.valueOf(datas[4]));
				obj.setHr(Integer.valueOf(datas[5]));
				obj.setHoliday(Integer.valueOf(datas[6]));
				obj.setWeekday(Integer.valueOf(datas[7]));
				obj.setWorkingday(Integer.valueOf(datas[8]));
				obj.setWeathersit(Integer.valueOf(datas[9]));
				obj.setTemp(Double.valueOf(datas[10]));
				obj.setAtemp(Double.valueOf(datas[11]));
				obj.setHum(Double.valueOf(datas[12]));
				obj.setWindspeed(Double.valueOf(datas[13]));
				obj.setCasual(Integer.valueOf(datas[14]));
				obj.setRegistered(Integer.valueOf(datas[15]));
				obj.setCnt(Integer.valueOf(datas[16]));
				return obj;
			}
		});
		System.out.println("hour count:"+hours.count());
		JavaRDD<String[]> records = hrecords.map(new Function<String, String[]>() {
			private static final long serialVersionUID = 4297254421745124107L;
			@Override
			public String[] call(String arg) {
				return arg.split(",");
			}
		});
		records.cache();
		System.out.println("hours count:"+records.count());
		JavaRDD<String[]> _data = records.map(new Function<String[], String[]>() {
			private static final long serialVersionUID = 4297254421745124107L;
			@Override
			public String[] call(String[] arg) {
				String[] target = new String[13];
				for(int i=2;i<arg.length;i++){
					if(i>1&&i<14){
						target[i-2]=arg[i];
					}
					if(i==16){
						target[12]=arg[i];
					}
				}
				return target;
			}
		});
		System.out.println(JSON.toJSONString(_data.distinct().zipWithIndex().collectAsMap()));
		int cat_len=0;
		List<Map<String, Long>> mappings = new ArrayList<Map<String,Long>>();
		for(int i=2;i<10;i++){
			Map<String, Long> map = get_mapping(hrecords, i);
			mappings.add(map);
			cat_len+=map.size();
		}
		int num_len = Arrays.asList(records.first()).subList(11, 15).size();
		final int total_len=num_len+cat_len;
		System.out.println("cat_len:"+cat_len);
		System.out.println("num_len:"+num_len);
		System.out.println("total_len:"+total_len);
		JavaRDD<LabeledPoint> data = _data.map(new Function<String[], LabeledPoint>() {
			private static final long serialVersionUID = 4297254421745124107L;
			@Override
			public LabeledPoint call(String[] arg) {
				double label = Integer.valueOf(arg[arg.length-1]);
				double[]  features = new double[total_len];
				for(int i=0,step=0;i<arg.length-1;i++){
					String field = arg[i];
					if(i<arg.length-1-4){
						Map<String, Long> m = mappings.get(i);
						Long idx = m.get(field);
						//------>
						features[step+idx.intValue()]=1;
						//等价于
//						double[] t= new double[m.size()];
//						t[idx.intValue()]=1;
//						System.arraycopy(t, 0, features, step, m.size());
						//<------------------
						step = step + m.size();
					}else{
						features[total_len-4+(i%4)] = Double.valueOf(field);
					}
				}
				return new LabeledPoint(label, Vectors.dense(features));
			}
		});
		JavaRDD<LabeledPoint> data_dt = _data.map(new Function<String[], LabeledPoint>() {
			private static final long serialVersionUID = 4297254421745124107L;
			@Override
			public LabeledPoint call(String[] arg) {
				double label = Integer.valueOf(arg[arg.length-1]);
				double[] features = new double[arg.length-1];
				for(int i=0;i<arg.length-1;i++){
					String field = arg[i];
					features[i] = Double.valueOf(field);
				}
				return new LabeledPoint(label, Vectors.dense(features));
			}
		});
		LabeledPoint first_point = data.first();
		System.out.println("Label:"+first_point.label());
		System.out.println("features:"+first_point.features());
		System.out.println("features len:"+first_point.features().size());
		System.out.println("------------------------------------------------回归模型[start]------------------------------------------------");
		int iterations=10;
		double step=0.1;
		LinearRegressionModel linear_model= LinearRegressionWithSGD.train(data.rdd(), iterations,step, 1.0);
		Map<Integer,Integer> categoricalFeaturesInfo=new HashMap<Integer, Integer>();
//		scala.collection.immutable.HashMap<Object, Object> categoricalFeaturesInfo = new scala.collection.immutable.HashMap<Object, Object>();
		String impurity="variance";//gini | entropy | variance
		int maxDepth=5;
		int maxBins=32;//固定划分数
//		DecisionTreeModel dt_model = DecisionTree.trainRegressor(data_dt.rdd(), categoricalFeaturesInfo, impurity, maxDepth, maxBins);
		DecisionTreeModel dt_model = DecisionTree.trainRegressor(data_dt, categoricalFeaturesInfo, impurity, maxDepth, maxBins);
		
		System.out.println("================================================模型预测[1s]================================================");
		JavaRDD<Tuple2<Double, Double>> linear_actual_predicted = data.map(new Function<LabeledPoint,Tuple2<Double,Double>>(){
			private static final long serialVersionUID = 1238103268181198570L;
			@Override
			public Tuple2<Double, Double> call(LabeledPoint point) throws Exception {
				double label = point.label();
				double score = linear_model.predict(point.features());
				return new Tuple2<Double, Double>(label,score);
			}
		});
		System.out.println("Linear Model predictions:"+JSON.toJSONString(linear_actual_predicted.take(5)));
		JavaRDD<Vector> features = data_dt.map(new Function<LabeledPoint, Vector>() {
			private static final long serialVersionUID = 343065739307853540L;
			@Override
			public Vector call(LabeledPoint point) {
				return point.features();
			}
		});
		JavaRDD<Double> dt_predicted = dt_model.predict(features);
		JavaRDD<Double> dt_actual = data.map(new Function<LabeledPoint, Double>() {
			private static final long serialVersionUID = -7748287683052254432L;
			@Override
			public Double call(LabeledPoint point) {
				return point.label();
			}
		});
		JavaPairRDD<Double, Double>	dt_actual_predicted = dt_actual.zip(dt_predicted);
		System.out.println("Decision Tree predictions:"+JSON.toJSONString(dt_actual_predicted.take(5)));
		System.out.println("Decision Tree depth:"+dt_model.depth());
		System.out.println("Decision Tree number of nodes:"+JSON.toJSONString(dt_model.numNodes()));
		System.out.println("================================================模型预测[1e]================================================");
		System.out.println("================================================模型评估[2s]================================================");
		// 均方误差
		double MSE = linear_actual_predicted.mapToDouble(new DoubleFunction<Tuple2<Double,Double>>(){
			private static final long serialVersionUID = 5141366646799931178L;
			@Override
			public double call(Tuple2<Double, Double> t) throws Exception {
				double actual = t._1();
				double pred = t._2();
				return squared_error(actual, pred);
			}
		}).mean();
		//均方根误差
		double RMSE = Math.sqrt(MSE);
		// 平均绝对误差
		double MAE = linear_actual_predicted.mapToDouble(new DoubleFunction<Tuple2<Double,Double>>(){
			private static final long serialVersionUID = 5141366646799931178L;
			@Override
			public double call(Tuple2<Double, Double> t) throws Exception {
				double actual = t._1();
				double pred = t._2();
				return abs_error(actual, pred);
			}
		}).mean();
		// 均方根对数误差
		double RMSLE = Math.sqrt(linear_actual_predicted.mapToDouble(new DoubleFunction<Tuple2<Double,Double>>(){
			private static final long serialVersionUID = 5141366646799931178L;
			@Override
			public double call(Tuple2<Double, Double> t) throws Exception {
				double actual = t._1();
				double pred = t._2();
				return squared_log_error(actual, pred);
			}
		}).mean());
		System.out.println("Linear Model -MSE:"+MSE);
		System.out.println("Linear Model -RMSE:"+RMSE);
		System.out.println("Linear Model -MAE:"+MAE);
		System.out.println("Linear Model -RMSLE:"+RMSLE);
		// 均方误差
		double MSE_dt = dt_actual_predicted.mapToDouble(new DoubleFunction<Tuple2<Double,Double>>(){
			private static final long serialVersionUID = 5141366646799931178L;
			@Override
			public double call(Tuple2<Double, Double> t) throws Exception {
				double actual = t._1();
				double pred = t._2();
				return squared_error(actual, pred);
			}
		}).mean();
		//均方根误差
		double RMSE_dt = Math.sqrt(MSE_dt);
		// 平均绝对误差
		double MAE_dt = dt_actual_predicted.mapToDouble(new DoubleFunction<Tuple2<Double,Double>>(){
			private static final long serialVersionUID = 5141366646799931178L;
			@Override
			public double call(Tuple2<Double, Double> t) throws Exception {
				double actual = t._1();
				double pred = t._2();
				return abs_error(actual, pred);
			}
		}).mean();
		// 均方根对数误差
		double RMSLE_dt = Math.sqrt(dt_actual_predicted.mapToDouble(new DoubleFunction<Tuple2<Double,Double>>(){
			private static final long serialVersionUID = 5141366646799931178L;
			@Override
			public double call(Tuple2<Double, Double> t) throws Exception {
				double actual = t._1();
				double pred = t._2();
				return squared_log_error(actual, pred);
			}
		}).mean());
		System.out.println("Decision Tree -MSE:"+MSE_dt);
		System.out.println("Decision Tree -RMSE:"+RMSE_dt);
		System.out.println("Decision Tree -MAE:"+MAE_dt);
		System.out.println("Decision Tree -RMSLE:"+RMSLE_dt);		
		System.out.println("================================================模型评估[2e]================================================");
		System.out.println("================================================参数调优[3s]================================================");
		JavaRDD<Tuple2<Long, LabeledPoint>> data_with_idx = data.zipWithIndex().map(new Function<Tuple2<LabeledPoint,Long>,Tuple2<Long,LabeledPoint>>(){
			private static final long serialVersionUID = 8182783288861212310L;
			@Override
			public Tuple2<Long, LabeledPoint> call(Tuple2<LabeledPoint, Long> v1) throws Exception {
				return new Tuple2<Long, LabeledPoint>(v1._2(),v1._1());
			}
		});
		long seed = 42;
		JavaRDD<Tuple2<Long, LabeledPoint>>[] train_test = data_with_idx.randomSplit(new double[]{0.2,0.8}, seed);
		JavaRDD<Tuple2<Long, LabeledPoint>> _test = train_test[0];
		JavaRDD<Tuple2<Long, LabeledPoint>> _train = train_test[1];
		JavaRDD<Tuple2<Long, LabeledPoint>> test = data_with_idx.sample(false, 0.2, seed);
		JavaRDD<Tuple2<Long, LabeledPoint>> train = data_with_idx.subtract(test);
		JavaRDD<LabeledPoint> _test_data = _test.map(new Function<Tuple2<Long, LabeledPoint>,LabeledPoint>(){
			private static final long serialVersionUID = 4613316047532718071L;
			@Override
			public LabeledPoint call(Tuple2<Long, LabeledPoint> v1) throws Exception {
				return v1._2();
			}
		});
		JavaRDD<LabeledPoint> _train_data = _train.map(new Function<Tuple2<Long, LabeledPoint>,LabeledPoint>(){
			private static final long serialVersionUID = 5310198354857479304L;
			@Override
			public LabeledPoint call(Tuple2<Long, LabeledPoint> v1) throws Exception {
				return v1._2();
			}
		});
		JavaRDD<LabeledPoint> test_data = test.map(new Function<Tuple2<Long, LabeledPoint>,LabeledPoint>(){
			private static final long serialVersionUID = -3178611363562459560L;
			@Override
			public LabeledPoint call(Tuple2<Long, LabeledPoint> v1) throws Exception {
				return v1._2();
			}
		});
		JavaRDD<LabeledPoint> train_data = train.map(new Function<Tuple2<Long, LabeledPoint>,LabeledPoint>(){
			private static final long serialVersionUID = -2845597819424633642L;
			@Override
			public LabeledPoint call(Tuple2<Long, LabeledPoint> v1) throws Exception {
				return v1._2();
			}
		});
		System.out.println("Linear--test->train size:"+_test_data.count()+"-->"+_train_data.count());
		System.out.println("Linear--test->train size:"+test_data.count()+"-->"+train_data.count());
		System.out.println("Linear--Total size:"+data.count());
		int[] param={1, 5, 10, 20, 50, 100};//最优迭代次数:20
		double[] rmsles = new double[param.length];
		for(int i=0;i<param.length;i++){
			double rmsle = evaluate_lr(train_data, test_data, param[i], 0.01, 0.0,"l2",false);
			rmsles[i]=rmsle;
		}
		System.out.println("Linear--RMSLE[numIterations]:\n"+JSON.toJSONString(param)+"\n"+JSON.toJSONString(rmsles));
		double[] _param={0.01, 0.025, 0.05, 0.1, 1.0};//最优步长:0.025
		rmsles = new double[_param.length];
		for(int i=0;i<_param.length;i++){
			double rmsle = evaluate_lr(train_data, test_data, 10, _param[i], 0.0,"l2",false);
			rmsles[i]=rmsle;
		}
		System.out.println("Linear--RMSLE[step]:\n"+JSON.toJSONString(_param)+"\n"+JSON.toJSONString(rmsles));
		System.out.println("-----------------------------------------------------------------------------------------------------------------");
		JavaRDD<Tuple2<Long, LabeledPoint>> data_with_idx_dt = data_dt.zipWithIndex().map(new Function<Tuple2<LabeledPoint,Long>,Tuple2<Long,LabeledPoint>>(){
			private static final long serialVersionUID = -8679040806842183297L;
			@Override
			public Tuple2<Long, LabeledPoint> call(Tuple2<LabeledPoint, Long> v1) throws Exception {
				return new Tuple2<Long, LabeledPoint>(v1._2(),v1._1());
			}
		});
		JavaRDD<Tuple2<Long, LabeledPoint>>[] train_test_dt = data_with_idx_dt.randomSplit(new double[]{0.2,0.8}, seed);
		JavaRDD<Tuple2<Long, LabeledPoint>> _test_dt = train_test_dt[0];
		JavaRDD<Tuple2<Long, LabeledPoint>> _train_dt = train_test_dt[1];
		JavaRDD<Tuple2<Long, LabeledPoint>> test_dt = data_with_idx_dt.sample(false, 0.2, seed);
		JavaRDD<Tuple2<Long, LabeledPoint>> train_dt = data_with_idx_dt.subtract(test_dt);
		JavaRDD<LabeledPoint> _test_data_dt = _test_dt.map(new Function<Tuple2<Long, LabeledPoint>,LabeledPoint>(){
			private static final long serialVersionUID = -1092268319094038891L;
			@Override
			public LabeledPoint call(Tuple2<Long, LabeledPoint> v1) throws Exception {
				return v1._2();
			}
		});
		JavaRDD<LabeledPoint> _train_data_dt = _train_dt.map(new Function<Tuple2<Long, LabeledPoint>,LabeledPoint>(){
			private static final long serialVersionUID = 8669640321496562442L;
			@Override
			public LabeledPoint call(Tuple2<Long, LabeledPoint> v1) throws Exception {
				return v1._2();
			}
		});
		JavaRDD<LabeledPoint> test_data_dt = test_dt.map(new Function<Tuple2<Long, LabeledPoint>,LabeledPoint>(){
			private static final long serialVersionUID = 2739767931408987926L;
			@Override
			public LabeledPoint call(Tuple2<Long, LabeledPoint> v1) throws Exception {
				return v1._2();
			}
		});
		JavaRDD<LabeledPoint> train_data_dt = train_dt.map(new Function<Tuple2<Long, LabeledPoint>,LabeledPoint>(){
			private static final long serialVersionUID = -6845378058200239550L;
			@Override
			public LabeledPoint call(Tuple2<Long, LabeledPoint> v1) throws Exception {
				return v1._2();
			}
		});
		System.out.println("Tree--test->train size:"+_test_data_dt.count()+"-->"+_train_data_dt.count());
		System.out.println("Tree--test->train size:"+test_data_dt.count()+"-->"+train_data_dt.count());
		System.out.println("Tree--Total size:"+data_dt.count());
		int[] dparam={1, 2, 3, 4, 5, 10, 20};//树最优深度:10
		rmsles = new double[dparam.length];
		for(int i=0;i<dparam.length;i++){
			double rmsle = evaluate_dt(train_data_dt, test_data_dt, dparam[i], maxBins);
			rmsles[i]=rmsle;
		}
		System.out.println("Tree--RMSLE[maxDepth]:\n"+JSON.toJSONString(dparam)+"\n"+JSON.toJSONString(rmsles));
		dparam=new int[]{8, 16,18, 20,22,26, 32, 64, 100};//最优划分数:20
		rmsles = new double[dparam.length];
		for(int i=0;i<dparam.length;i++){
			double rmsle = evaluate_dt(train_data_dt, test_data_dt,maxDepth, dparam[i]);
			rmsles[i]=rmsle;
		}
		System.out.println("Tree--RMSLE[maxBins]:\n"+JSON.toJSONString(dparam)+"\n"+JSON.toJSONString(rmsles));
		System.out.println("================================================参数调优[3e]================================================");
		System.out.println("------------------------------------------------回归模型[end]------------------------------------------------");
		long end = System.currentTimeMillis();
		double time = (end - start) / 1000.00;
		double ss = time % 60;
		int mm = Double.valueOf(time / 60).intValue() % 60;
		int hh = Double.valueOf(time / 60 / 60).intValue() % 60;
		System.out.println("-------------------------regression耗时:"+(hh>0?hh+"小时":"")+(mm>0?mm+"分钟":"")+ss+"秒"+"-------------------------");
		sc.close();
		System.exit(1);
	}
	/**
	 * @decription 二元编码
	 * @author yi.zhang
	 * @time 2017年7月5日 下午3:53:57
	 * @param rdd
	 * @param dx
	 * @return
	 */
	public static Map<String, Long> get_mapping(JavaRDD<String> rdd,int dx){
		Map<String, Long> map = rdd.map(new Function<String, String>() {
			private static final long serialVersionUID = 1L;
			@Override
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
		LinearRegressionModel model = LinearRegressionWithSGD.train(train.rdd(), iterations, step, 1.0);
		JavaRDD<Tuple2<Double, Double>> tp = test.map(new Function<LabeledPoint,Tuple2<Double,Double>>(){
			private static final long serialVersionUID = 2164755036956258861L;
			@Override
			public Tuple2<Double, Double> call(LabeledPoint v1) throws Exception {
				double actual = v1.label();
				double pred = model.predict(v1.features());
				return new Tuple2<Double, Double>(actual,pred);
			}
		});
		double rmsle = Math.sqrt(tp.mapToDouble(new DoubleFunction<Tuple2<Double,Double>>(){
			private static final long serialVersionUID = 1909735037664834392L;
			@Override
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
			@Override
			public Vector call(LabeledPoint v1) throws Exception {
				return v1.features();
			}
		}));
		JavaRDD<Double> actual = test.map(new Function<LabeledPoint,Double>(){
			private static final long serialVersionUID = -5768569081281096187L;
			@Override
			public Double call(LabeledPoint v1) throws Exception {
				return v1.label();
			}
		});
		JavaPairRDD<Double, Double>	tp = actual.zip(preds);
		double rmsle = Math.sqrt(tp.mapToDouble(new DoubleFunction<Tuple2<Double,Double>>(){
			private static final long serialVersionUID = -8908824130626102171L;
			@Override
			public double call(Tuple2<Double, Double> t) throws Exception {
				double actual = t._1();
				double pred = t._2();
				return squared_log_error(actual, pred);
			}
		}).mean());
		return rmsle;
	}
	/**
	 * @decription 聚类模型(无监督学习)
	 * @author yi.zhang
	 * @time 2017年7月6日 下午6:27:15
	 */
	public void cluster(){
		long start = System.currentTimeMillis();
		String master = "local[2]";// 主节点运行模式: local | yarn-client | yarn-standalone
		String appName = "SparkMLTest";// 应用名称
		String spark_home = System.getenv("SPARK_HOME");
		System.out.println("-------------------------------------------------------------------"+spark_home);
		JavaSparkContext sc = new JavaSparkContext(master, appName);
//		sc.setLogLevel("info");
		sc.getConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
		sc.getConf().registerKryoClasses(new Class[] { JavaSerializer.class,KryoSerializer.class,GenericAvroSerializer.class });
		String path = System.getProperty("user.dir") + "/src/main/resources"+"/data/ml-100k";
		JavaRDD<String> user_data = sc.textFile(path + "/u.user");
		JavaRDD<Consumer> users = user_data.map(new Function<String, Consumer>() {
			private static final long serialVersionUID = 5689548223086341309L;
			@Override
			public Consumer call(String arg) {
				String[] datas = arg.split("\\|");
				Consumer user = new Consumer();
				user.setId(Integer.valueOf(datas[0]));
				user.setAge(datas.length>1?Integer.valueOf(datas[1]):0);
				user.setGender(datas.length>2?Character.valueOf(datas[2].charAt(0)):0);
				user.setDuty(datas.length>3?datas[3]:null);
				user.setCode(datas.length>4?datas[4]:null);
				return user;
			}
		});
		Map<Integer,Consumer> user_map = new HashMap<Integer,Consumer>();
		for(Consumer user:users.collect()){
			user_map.put(user.getId(), user);
		}
		JavaRDD<String> movie_data = sc.textFile(path + "/u.item");
		JavaRDD<Film> movies = movie_data.map(new Function<String, Film>() {
			private static final long serialVersionUID = -3964625132188595257L;
			@Override
			public Film call(String arg) {
				String[] datas = arg.split("\\|");
				Film movie = new Film();
				movie.setId(Integer.valueOf(datas[0]));
				movie.setTitle(datas.length>1?datas[1]:null);
				movie.setDate(datas.length>2?DateUtil.formatDateTime(datas[2], "dd-MMM-yyyy"):null);
				movie.setUrl(datas.length>4?datas[4]:null);
				return movie;
			}
		});
		Map<Integer,Film> movie_map = new HashMap<Integer,Film>();
		for(Film movie:movies.collect()){
			movie_map.put(movie.getId(), movie);
		}
		JavaRDD<String> level_data = sc.textFile(path + "/u.data");
		JavaRDD<Grade> levels = level_data.map(new Function<String, Grade>() {
			private static final long serialVersionUID = 1978593093871408976L;
			@Override
			public Grade call(String arg) {
				String[] datas = arg.split("\t");
				Grade level = new Grade();
				level.setUserId(Integer.valueOf(datas[0]));
				level.setMovieId(datas.length>1?Integer.valueOf(datas[1]):0);
				level.setScore(datas.length>2?Integer.valueOf(datas[2]):0);
				level.setCreateTime(datas.length>3?new Date(Long.valueOf(datas[3])):null);
				return level;
			}
		});
		Map<String,Double> level_map = new HashMap<String,Double>();
		for(Grade level:levels.collect()){
			level_map.put(level.getUserId()+"_"+level.getMovieId(), level.getScore());
		}
		JavaRDD<String> genre_data = sc.textFile(path + "/u.genre");//电影题材
		JavaRDD<Genre> genres = genre_data.map(new Function<String, Genre>() {
			private static final long serialVersionUID = -8672505213077863116L;
			@Override
			public Genre call(String arg) {
				String[] datas = arg.split("\\|");
				Genre obj = new Genre();
				obj.setMaterial(datas[0]);
				obj.setIndex(datas.length>1?Integer.valueOf(datas[1]):0);
				return obj;
			}
		});
		Map<Integer,Genre> genre_map = new HashMap<Integer,Genre>();
		for(Genre obj:genres.collect()){
			genre_map.put(obj.getIndex(), obj);
		}
		Map<String, String> genreMap = genre_data.filter(new Function<String, Boolean>(){
			private static final long serialVersionUID = 3377187581103244239L;
			@Override
			public Boolean call(String v1) throws Exception {
				return !StringUtil.isEmptyStr(v1);
			}
		}).mapToPair(new PairFunction<String,String,String>(){
			private static final long serialVersionUID = 5223073610995175267L;
			@Override
			public Tuple2<String, String> call(String arg) throws Exception {
				String[] datas = arg.split("\\|");
				String material = datas[0];
				int index = datas.length>1?Integer.valueOf(datas[1]):0;
				return new Tuple2<String, String>(index+"",material);
			}
		}).collectAsMap();
//		System.out.println(JSON.toJSONString(genreMap));
//		JavaRDD<Tuple2<Integer, Tuple2<String, List<String>>>> titlesAndGenres = movie_data.map(new Function<String,Tuple2<Integer, Tuple2<String, List<String>>>>(){
//			private static final long serialVersionUID = -2050550930501081824L;
//			@Override
//			public Tuple2<Integer, Tuple2<String, List<String>>> call(String arg) throws Exception {
//				String[] datas = arg.split("\\|");
//				int id = Integer.valueOf(datas[0]);
//				String title = datas[1];
//				String[] genres = Arrays.copyOfRange(datas, 5, datas.length);
//				List<String> genresAssigned = sc.parallelize(Arrays.asList(genres)).zipWithIndex().filter(new Function<Tuple2<String, Long>, Boolean>(){
//					private static final long serialVersionUID = 7438874609511992942L;
//					@Override
//					public Boolean call(Tuple2<String, Long> v1) throws Exception {
//						return "1".equals(v1._1());
//					}
//				}).map(new Function<Tuple2<String, Long>, String>(){
//					private static final long serialVersionUID = -5524381284690280180L;
//
//					public String call(Tuple2<String, Long> v1) throws Exception {
//						return genreMap.get(v1._2().toString());
//					}
//				}).filter(new Function<String,Boolean>(){
//					private static final long serialVersionUID = 5675262159070661970L;
//					@Override
//					public Boolean call(String v1) throws Exception {
//						return v1!=null;
//					}
//				}).collect();
//				return new Tuple2<Integer, Tuple2<String, List<String>>>(id,new Tuple2<String, List<String>>(title,genresAssigned));
//			}
//		});
		List<String> genresAssigned = movie_data.flatMap(new FlatMapFunction<String,String>(){
			private static final long serialVersionUID = 7687017255830826628L;
			@Override
			public Iterator<String> call(String arg) throws Exception {
				String[] datas = arg.split("\\|");
				String[] genres = Arrays.copyOfRange(datas, 5, datas.length);
				return Arrays.asList(genres).iterator();
			}
		}).zipWithIndex().filter(new Function<Tuple2<String, Long>, Boolean>(){
			private static final long serialVersionUID = 7438874609511992942L;
			@Override
			public Boolean call(Tuple2<String, Long> v1) throws Exception {
				return "1".equals(v1._1());
			}
		}).map(new Function<Tuple2<String, Long>, String>(){
			private static final long serialVersionUID = -5524381284690280180L;
			@Override
			public String call(Tuple2<String, Long> v1) throws Exception {
				return genreMap.get(v1._2().toString());
			}
		}).filter(new Function<String,Boolean>(){
			private static final long serialVersionUID = -6849912697722380821L;
			@Override
			public Boolean call(String v1) throws Exception {
				return v1!=null;
			}
		}).collect();
		JavaPairRDD<Integer, Tuple2<String, List<String>>> titlesAndGenres = movie_data.mapToPair(new PairFunction<String,Integer, Tuple2<String, List<String>>>(){
			private static final long serialVersionUID = -2050550930501081824L;
			@Override
			public Tuple2<Integer, Tuple2<String, List<String>>> call(String arg) throws Exception {
				String[] datas = arg.split("\\|");
				int id = Integer.valueOf(datas[0]);
				String title = datas[1];
				return new Tuple2<Integer, Tuple2<String, List<String>>>(id,new Tuple2<String, List<String>>(title,genresAssigned));
			}
		});
		System.out.println(titlesAndGenres.first());
		JavaRDD<Rating> ratings = level_data.map(new Function<String,Rating>(){
			private static final long serialVersionUID = -5132086128010665193L;
			@Override
			public Rating call(String arg) throws Exception {
				String[] datas = arg.split("\t");
				int userId = Integer.valueOf(datas[0]);
				int filmId = datas.length>1?Integer.valueOf(datas[1]):0;
				int score = datas.length>2?Integer.valueOf(datas[2]):0;
				return new Rating(userId, filmId, score);
			}
		});
		JavaRDD<Rating> _ratings = ratings.cache();
		MatrixFactorizationModel alsModel = ALS.train(_ratings.rdd(), 50, 10, 0.1);
		JavaPairRDD<Integer, Vector> movieFactors = alsModel.productFeatures().toJavaRDD().mapToPair(new PairFunction<Tuple2<Object, double[]>,Integer,Vector>(){
			private static final long serialVersionUID = 5315883387394936932L;
			@Override
			public Tuple2<Integer,Vector> call(Tuple2<Object, double[]> v1) throws Exception {
				Vector features= Vectors.dense(v1._2());
				return new Tuple2<Integer,Vector>((Integer)v1._1(),features);
			}
		});
		JavaRDD<Vector> movieVectors = movieFactors.map(new Function<Tuple2<Integer, Vector>, Vector>(){
			private static final long serialVersionUID = -4968107214927890249L;
			@Override
			public Vector call(Tuple2<Integer, Vector> v1) throws Exception {
				// TODO Auto-generated method stub
				return v1._2();
			}
		});
		JavaPairRDD<Object, Vector> userFactors = alsModel.userFeatures().toJavaRDD().mapToPair(new PairFunction<Tuple2<Object, double[]>,Object,Vector>(){
			private static final long serialVersionUID = -3663337467837727879L;
			@Override
			public Tuple2<Object,Vector> call(Tuple2<Object, double[]> v1) throws Exception {
				Vector features= Vectors.dense(v1._2());
				return new Tuple2<Object,Vector>(v1._1(),features);
			}
		});
		JavaRDD<Vector> userVectors = userFactors.map(new Function<Tuple2<Object, Vector>, Vector>(){
			private static final long serialVersionUID = -3327975822610086798L;
			@Override
			public Vector call(Tuple2<Object, Vector> v1) throws Exception {
				return v1._2();
			}
		});
//		RowMatrix movieMatrix = new RowMatrix(movieVectors.rdd());
//		MultivariateStatisticalSummary movieMatrixSummary = movieMatrix.computeColumnSummaryStatistics();
//		RowMatrix userMatrix = new RowMatrix(userVectors.rdd());
//		MultivariateStatisticalSummary userMatrixSummary = userMatrix.computeColumnSummaryStatistics();
//		System.out.println("Movie factors mean: " + movieMatrixSummary.mean());
//		System.out.println("Movie factors variance: " + movieMatrixSummary.variance());
//		System.out.println("User factors mean: " + userMatrixSummary.mean());
//		System.out.println("User factors variance: " + userMatrixSummary.variance());
		int numClusters = 5;//类族数
		int numIterations = 10;//迭代次数
		int numRuns = 3;//训练次数
		//K-均值模型
		KMeansModel movieClusterModel = KMeans.train(movieVectors.rdd(), numClusters, numIterations, numRuns);
		KMeansModel userClusterModel = KMeans.train(userVectors.rdd(), numClusters, numIterations, numRuns);
//		KMeansModel movieClusterModelConverged = KMeans.train(movieVectors.rdd(),numClusters, 100);
//		System.out.println(movieClusterModel);
//		System.out.println(userClusterModel);
		
		System.out.println("------------------------------------------------聚类模型[start]------------------------------------------------");
		System.out.println("================================================模型预测[1s]================================================");
		JavaRDD<Integer> predictions = movieClusterModel.predict(movieVectors);
		System.out.println(predictions.take(10));
		JavaPairRDD<Integer, Tuple2<Tuple2<String, List<String>>, Vector>> titlesWithFactors = titlesAndGenres.join(movieFactors);
		JavaRDD<Tuple5<Integer, String, String, Integer, Double>> moviesAssigned = titlesWithFactors.map(new Function<Tuple2<Integer, Tuple2<Tuple2<String, List<String>>, Vector>>,Tuple5<Integer,String,String,Integer,Double>>(){
			private static final long serialVersionUID = 4047979801661388523L;
			@Override
			public Tuple5<Integer,String,String,Integer,Double> call(Tuple2<Integer, Tuple2<Tuple2<String, List<String>>, Vector>> v1) throws Exception {
				int id = v1._1();
				Vector vector = v1._2()._2();
				String title = v1._2()._1()._1();
				List<String> genres = v1._2()._1()._2();
				int pred = movieClusterModel.predict(vector);//预测从属那个类族
				Vector[] clusterCentre = movieClusterModel.clusterCenters();
//				double dist = computeDistance(new DenseVector<Double>(clusterCentre), new DenseVector<Double>(vector));
				return new Tuple5<Integer,String,String,Integer,Double>(id,title,org.apache.commons.lang.StringUtils.join(genres, " "),pred,1.0);
			}
		});                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
		Map<Integer, Iterable<Tuple5<Integer, String, String, Integer, Double>>> clusterAssignments = moviesAssigned.groupBy(new Function<Tuple5<Integer, String, String, Integer, Double>, Integer>(){
			private static final long serialVersionUID = 607921126140547309L;
			@Override
			public Integer call(Tuple5<Integer, String, String, Integer, Double> v1) throws Exception {
				return v1._4();
			}
		}).collectAsMap();
		for (Integer key : clusterAssignments.keySet()) {
			System.out.println(String.format("Cluster %d:", key));
//			Iterable<Tuple5<Integer, String, String, Integer, Double>> value = clusterAssignments.get(key);
//			value.forEach(obj->{
//				String title = obj._2();
//				String genre = obj._3();
//				double dist = obj._5();
//				System.out.println(title+"-->"+genre+"-->"+dist);
//			});
		}
		System.out.println("================================================模型预测[1e]================================================");
		System.out.println("================================================模型评估[2s]================================================");
		double movieCost = movieClusterModel.computeCost(movieVectors.rdd());
		double userCost = userClusterModel.computeCost(userVectors.rdd());
		System.out.println("movie Wshare:"+movieCost+",user Wshare:"+userCost);//K-元件的目标函数
		System.out.println("================================================模型评估[2e]================================================");
		System.out.println("================================================参数调优[3s]================================================");
		int[] Cluster={2,3,4,5,10,20,30,50};//最优K值10
		JavaRDD<Vector>[] trainTestSplitMovies = movieVectors.randomSplit(new double[]{0.6, 0.4}, 123);
		JavaRDD<Vector> trainMovies = trainTestSplitMovies[0];
		JavaRDD<Vector> testMovies = trainTestSplitMovies[1];
		for (int k: Cluster) {
			KMeansModel model = KMeans.train(trainMovies.rdd(), numIterations, k, numRuns);
			System.out.println("Cluster["+k+"]movie Wshare:"+model.computeCost(testMovies.rdd()));
		}
		JavaRDD<Vector>[] trainTestSplitUsers = userVectors.randomSplit(new double[]{0.6, 0.4}, 123);
		JavaRDD<Vector> trainUsers = trainTestSplitUsers[0];
		JavaRDD<Vector> testUsers = trainTestSplitUsers[1];
		for (int k: Cluster) {
			KMeansModel model = KMeans.train(trainUsers.rdd(), numIterations, k, numRuns);
			System.out.println("Cluster["+k+"]user Wshare:"+model.computeCost(testUsers.rdd()));
		}
		System.out.println("================================================参数调优[3e]================================================");
		System.out.println("------------------------------------------------聚类模型[end]------------------------------------------------");
		long end = System.currentTimeMillis();
		double time = (end - start) / 1000.00;
		double ss = time % 60;
		int mm = Double.valueOf(time / 60).intValue() % 60;
		int hh = Double.valueOf(time / 60 / 60).intValue() % 60;
		System.out.println("-------------------------regression耗时:"+(hh>0?hh+"小时":"")+(mm>0?mm+"分钟":"")+ss+"秒"+"-------------------------");
		sc.close();
		System.exit(1);
	}
	public static double computeDistance(DenseVector<Double> v1,DenseVector<Double> v2){
//		v1-v2;new DoubleMatrix(v1.toDenseMatrix().)
//		return pow(v1-v2, 2).sum();
		return 0;
	}
	/**
	 * @decription 降维技术
	 * @author yi.zhang
	 * @throws Exception 
	 * @time 2017年7月10日 下午6:42:58
	 */
	public void dimension() throws Exception{
		long start = System.currentTimeMillis();
		String master = "local[2]";// 主节点运行模式: local | yarn-client | yarn-standalone
		String appName = "SparkMLTest";// 应用名称
		String spark_home = System.getenv("SPARK_HOME");
		System.out.println("-------------------------------------------------------------------"+spark_home);
		JavaSparkContext sc = new JavaSparkContext(master, appName);
		sc.setLogLevel("warn");
		sc.getConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
		sc.getConf().registerKryoClasses(new Class[] { JavaSerializer.class,KryoSerializer.class,GenericAvroSerializer.class });
		String path = System.getProperty("user.dir") + "/src/main/resources"+"/data/lfw";
		JavaPairRDD<String, String> data = sc.wholeTextFiles(path+"/*");
		System.out.println("------------------------------------------------降维技术[start]------------------------------------------------");
//		System.out.println(data.first());
		JavaRDD<String> files = data.map(new Function<Tuple2<String,String>,String>(){
			private static final long serialVersionUID = 2411011053612282577L;
			@Override
			public String call(Tuple2<String, String> v1) throws Exception {
				String fileName = v1._1();
				return fileName.replace("file:", "");
			}
		});
		System.out.println("lfw count:"+files.count());
		JavaRDD<BufferedImage> data_source = files.map(new Function<String,BufferedImage>(){
			private static final long serialVersionUID = 8966284497083036845L;
			@Override
			public BufferedImage call(String fileName) throws Exception {
				return ImageIO.read(new File(fileName));
			}
		});
		System.out.println(data_source.count());
		JavaRDD<BufferedImage> data_target = files.map(new Function<String,BufferedImage>(){
			private static final long serialVersionUID = -632049068315220305L;
			@Override
			public BufferedImage call(String fileName) throws Exception {
				File file = new File(fileName);
				BufferedImage image = ImageIO.read(file);
				int width = 100,height = 100;
				BufferedImage bwImage = new BufferedImage(width, height, BufferedImage.TYPE_BYTE_GRAY);
				Graphics g = bwImage.getGraphics();
				g.drawImage(image, 0, 0, width, height, null);
				g.dispose();
				File target = new File(SparkMLTest.class.getResource("/").getPath()+"/tmp/"+file.getName());
				if(!target.exists())target.mkdirs();
				ImageIO.write(bwImage, "jpg", target);
				return bwImage;
			}
		});
//		System.out.println(data_target.count());
		JavaRDD<double[]> pixels = data_target.map(new Function<BufferedImage,double[]>(){
			private static final long serialVersionUID = 8523535555092484476L;
			@Override
			public double[] call(BufferedImage image) throws Exception {
				int width = image.getWidth(),height = image.getHeight();
				double[] pixels = new double[width * height];
				pixels = image.getData().getPixels(0, 0, width, height, pixels);
				return pixels;
			}
		});
//		pixels.take(10).forEach(obj->{
//			System.out.println(JSON.toJSONString(obj));
//		});
		JavaRDD<Vector> vectors = pixels.map(new Function<double[],Vector>(){
			private static final long serialVersionUID = -7121545657615204964L;
			@Override
			public Vector call(double[] v1) throws Exception {
				// TODO Auto-generated method stub
				return Vectors.dense(v1);
			}
		});
		vectors.setName("image-vectors");
		vectors.cache();
		StandardScalerModel scaler = new StandardScaler(true, false).fit(vectors.rdd());
//		System.out.println(scaler.mean());
		JavaRDD<Vector> scaledVectors = vectors.map(new Function<Vector,Vector>(){
			private static final long serialVersionUID = -6435149777635569015L;
			@Override
			public Vector call(Vector v1) throws Exception {
				return scaler.transform(v1);
			}
		});
//		System.out.println(scaledVectors.count());
		RowMatrix matrix = new RowMatrix(scaledVectors.rdd());
		int K = 10;
		Matrix pc = matrix.computePrincipalComponents(K);
		int rows = pc.numRows();
		int cols = pc.numCols();
		System.out.println("rows:"+rows+",cols:"+cols);
//		DenseMatrix pcBreeze = new DenseMatrix(rows, cols, pc.toArray());
		RowMatrix projected = matrix.multiply(pc);
		System.out.println(projected.numRows()+","+ projected.numCols());
		SingularValueDecomposition<RowMatrix, Matrix> svd = matrix.computeSVD(K, true,1.0);
		System.out.println("--U-->rows:"+svd.U().numRows()+",cols:"+svd.U().numCols());//奇异向量
		System.out.println("--V-->rows:"+svd.V().numRows()+",cols:"+svd.V().numCols());//右奇异向量
		System.out.println("--s-->size:"+svd.s().size());//对角矩阵
		System.out.println("------------------------------------------------降维技术[end]------------------------------------------------");
		long end = System.currentTimeMillis();
		double time = (end - start) / 1000.00;
		double ss = time % 60;
		int mm = Double.valueOf(time / 60).intValue() % 60;
		int hh = Double.valueOf(time / 60 / 60).intValue() % 60;
		System.out.println("-------------------------regression耗时:"+(hh>0?hh+"小时":"")+(mm>0?mm+"分钟":"")+ss+"秒"+"-------------------------");
		sc.close();
		System.exit(1);
	}
	/**
	 * @decription 高级文本处理技术[java8]
	 * @author yi.zhang
	 * @time 2017年7月11日 下午3:46:27
	 */
	public void htext(){
		long start = System.currentTimeMillis();
		String master = "local[6]";// 主节点运行模式: local | yarn-client | yarn-standalone
		String appName = "SparkMLTest";// 应用名称
		String spark_home = System.getenv("SPARK_HOME");
		System.out.println("-------------------------------------------------------------------"+spark_home);
		JavaSparkContext sc = new JavaSparkContext(master, appName);
		sc.setLogLevel("warn");
		sc.getConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
		sc.getConf().registerKryoClasses(new Class[] { JavaSerializer.class,KryoSerializer.class,GenericAvroSerializer.class });
		String path = System.getProperty("user.dir") + "/src/main/resources"+"/data/news";
		JavaPairRDD<String, String> data = sc.wholeTextFiles(path+"/train/*");
		System.out.println("------------------------------------------------高级文本处理技术[start]java8------------------------------------------------");
		JavaRDD<String> text = data.map(new Function<Tuple2<String,String>,String>(){
			private static final long serialVersionUID = -8160739561030343422L;
			@Override
			public String call(Tuple2<String, String> v1) throws Exception {
				String text = v1._2();
				return text;
			}
		});
		text.cache();
		System.out.println("text count:"+text.count());
		@SuppressWarnings("resource")
		JavaRDD<String> newsgroups = data.map(v1->{ 
			String[] titles = v1._1().split("/"); 
			return titles[titles.length-2];
		});
		List<Tuple2<String, Long>> countByGroup = newsgroups.mapToPair(n->new Tuple2<String, Long>(n, 1l)).reduceByKey((n1,n2)->n1+n2).collect();
//		Collections.sort(countByGroup, (v1,v2)->v1._2().intValue()-v2._2().intValue());
		System.out.println(countByGroup);
		JavaRDD<String> whiteSpaceSplit = text.flatMap(t->Arrays.asList(t.split(" ")).iterator()).map(t->t.toLowerCase());
		System.out.println("word count:"+whiteSpaceSplit.distinct().count());
		System.out.println("100 word:"+StringUtils.join(whiteSpaceSplit.sample(true, 0.3, 42).take(100), ","));
		JavaRDD<String> nonWordSplit = text.flatMap(t->Arrays.asList(t.split("(\\W+)")).iterator()).map(t->t.toLowerCase());
		System.out.println("hword count:"+nonWordSplit.distinct().count());//130126
		System.out.println("100 hword:"+StringUtils.join(nonWordSplit.sample(true, 0.3, 42).take(100), ","));
		JavaRDD<String> filterNumbers = nonWordSplit.filter(t->t.matches("([^0-9]*)"));
		System.out.println("fword count:"+filterNumbers.distinct().count());//84912
		System.out.println("filter Num:"+StringUtils.join(filterNumbers.sample(true, 0.3, 42).take(100), ","));
		JavaPairRDD<String, Long> tokenCounts = filterNumbers.mapToPair(n->new Tuple2<String, Long>(n, 1l)).reduceByKey((n1,n2)->n1+n2);
		System.out.println("token count:"+tokenCounts.count());
		System.out.println("token reduce:"+tokenCounts.top(20, (v1,v2)->v2._2().intValue()-v1._2().intValue()));
		HashSet<String> stopwords = new HashSet<String>(Arrays.asList(new String[]{"the","a","an","of","or","in","for","by","on","but", "is", "not","with", "as", "was", "if","they", "are", "this", "and", "it", "have", "from", "at", "my","be", "that", "to"}));
		JavaPairRDD<String, Long> tokenCountsFilteredStopwords = tokenCounts.filter(v->!stopwords.contains(v._1()));
		System.out.println("stop count:"+tokenCountsFilteredStopwords.count());
		System.out.println("filter stop:"+JSON.toJSONString(tokenCountsFilteredStopwords.top(20, (v1,v2)->v2._2().intValue()-v1._2().intValue())));
		JavaPairRDD<String, Long> tokenCountsFilteredSize = tokenCountsFilteredStopwords.filter(v->v._1().length()>=2);
		System.out.println("size count:"+tokenCountsFilteredSize.count());
		System.out.println("filter size:"+tokenCountsFilteredSize.top(20));
		List<String> rareTokens = tokenCounts.filter(v->v._2()<2l).map(v->v._1()).collect();
		JavaPairRDD<String, Long> tokenCountsFilteredAll = tokenCountsFilteredSize.filter(v->!rareTokens.contains(v._1()));
		System.out.println("filter 2+ count:"+tokenCountsFilteredAll.count());
		System.out.println("filter 2+ size:"+tokenCountsFilteredAll.top(20));
		JavaRDD<String> _tokens = text.flatMap(doc -> tokenize(doc, rareTokens));
		System.out.println(_tokens.distinct().count());
		@SuppressWarnings("unchecked")
		JavaRDD<List<String>> tokens = text.map(doc -> IteratorUtils.toList(tokenize(doc, rareTokens)));
		System.out.println(tokens.first().subList(0, 20));
		int dim = Double.valueOf(Math.pow(2, 18)).intValue();
		HashingTF hashingTF = new HashingTF(dim);
		JavaRDD<Vector> tf = hashingTF.transform(tokens);
		tf.cache();
		SparseVector v = tf.first().toSparse();
		System.out.println(v.size());
		System.out.println(v.values().length);
		System.out.println(Arrays.asList(v.values()).subList(0, 10));
		System.out.println(Arrays.asList(v.indices()).subList(0, 10));
		IDFModel idf = new IDF().fit(tf);
		JavaRDD<Vector> tfidf = idf.transform(tf);
		SparseVector v2 = tfidf.first().toSparse();
		System.out.println(v2.size());
		System.out.println(v2.values().length);
		System.out.println(Arrays.asList(v2.values()).subList(0, 10));
		System.out.println(Arrays.asList(v2.indices()).subList(0, 10));
		@SuppressWarnings("resource")
		JavaRDD<Tuple2<Double,Double>> minMaxVals = tfidf.map(vector->{
			double[] values = vector.toSparse().values();
			double min = values[0], max = values[0];
			for (double value : values) {
				min = Math.min(min, value);
				max = Math.max(max, value);
			}
			return new Tuple2<Double, Double>(min, max);
		});
		Tuple2<Double, Double> globalMinMax = minMaxVals.reduce((m1,m2)->new Tuple2<Double, Double>(Math.min(m1._1(), m2._1()), Math.max(m1._2(), m2._2())));
		System.out.println(globalMinMax);
		
		JavaRDD<String> common = sc.parallelize(Arrays.asList(new String[]{"you","do","we"}));
		JavaRDD<Vector> tfCommon = hashingTF.transform(common.map(str->Arrays.asList(str)));
		JavaRDD<Vector> tfidfCommon = idf.transform(tfCommon);
		SparseVector commonVector = tfidfCommon.first().toSparse();
		System.out.println(commonVector.values());
		System.out.println("------------------------------------------------高级文本处理技术[end]java8------------------------------------------------");
		long end = System.currentTimeMillis();
		double time = (end - start) / 1000.00;
		double ss = time % 60;
		int mm = Double.valueOf(time / 60).intValue() % 60;
		int hh = Double.valueOf(time / 60 / 60).intValue() % 60;
		System.out.println("-------------------------regression耗时:"+(hh>0?hh+"小时":"")+(mm>0?mm+"分钟":"")+ss+"秒"+"-------------------------");
		sc.close();
		System.exit(1);
	}
	/**
	 * @decription 高级文本处理技术
	 * @author yi.zhang
	 * @time 2017年7月11日 下午3:46:27
	 */
	public void htext1(){
		long start = System.currentTimeMillis();
		String master = "local[2]";// 主节点运行模式: local | yarn-client | yarn-standalone
		String appName = "SparkMLTest";// 应用名称
		String spark_home = System.getenv("SPARK_HOME");
		System.out.println("-------------------------------------------------------------------"+spark_home);
		JavaSparkContext sc = new JavaSparkContext(master, appName);
		sc.setLogLevel("warn");
		sc.getConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
		sc.getConf().registerKryoClasses(new Class[] { JavaSerializer.class,KryoSerializer.class,GenericAvroSerializer.class });
		String path = System.getProperty("user.dir") + "/src/main/resources"+"/data/news";
		JavaPairRDD<String, String> data = sc.wholeTextFiles(path+"/train/*");
		System.out.println("------------------------------------------------高级文本处理技术[start]------------------------------------------------");
		JavaRDD<String> text = data.map(new Function<Tuple2<String,String>,String>(){
			private static final long serialVersionUID = -8160739561030343422L;
			@Override
			public String call(Tuple2<String, String> v1) throws Exception {
				String text = v1._2();
				return text;
			}
		});
		text.cache();
		System.out.println("text count:"+text.count());
		JavaRDD<String> newsgroups = data.map(new Function<Tuple2<String,String>, String>() {
			private static final long serialVersionUID = -2890858567317767248L;
			@Override
			public String call(Tuple2<String, String> v1) throws Exception {
				String[] titles = v1._1().split("/"); 
				return titles[titles.length-2];
			}
		});
		List<Tuple2<String, Long>> countByGroup = newsgroups.mapToPair(new PairFunction<String, String, Long>(){
			private static final long serialVersionUID = -4118589603915413127L;
			@Override
			public Tuple2<String, Long> call(String n) throws Exception {
				return new Tuple2<String, Long>(n, 1l);
			}
		}).reduceByKey(new Function2<Long, Long, Long>(){
			private static final long serialVersionUID = -779681914230644429L;
			@Override
			public Long call(Long n1, Long n2) throws Exception {
				return n1+n2;
			}
		}).collect();
//		Collections.sort(countByGroup, new SparkComparator<Tuple2<String, Long>>(){
//			private static final long serialVersionUID = 6771068790453852060L;
//			@Override
//			public int compare(Tuple2<String, Long> v1, Tuple2<String, Long> v2) {
//				return v2._2().intValue()-v1._2().intValue();
//			}
//		});
		System.out.println(countByGroup);
		JavaRDD<String> whiteSpaceSplit = text.flatMap(new FlatMapFunction<String, String>(){
			private static final long serialVersionUID = -414157744861772158L;
			@Override
			public Iterator<String> call(String t) throws Exception {
				return Arrays.asList(t.split(" ")).iterator();
			}
		}).map(new Function<String,String>(){
			private static final long serialVersionUID = -8904324843234842035L;
			@Override
			public String call(String v1) throws Exception {
				return v1.toLowerCase();
			}
		});
		System.out.println("word count:"+whiteSpaceSplit.distinct().count());
		System.out.println("100 word:"+StringUtils.join(whiteSpaceSplit.sample(true, 0.3, 42).take(100), ","));
		JavaRDD<String> nonWordSplit = text.flatMap(new FlatMapFunction<String, String>(){
			private static final long serialVersionUID = 8028027043186306937L;
			@Override
			public Iterator<String> call(String t) throws Exception {
				return Arrays.asList(t.split("(\\W+)")).iterator();//过滤符号
			}
		}).map(new Function<String,String>(){
			private static final long serialVersionUID = 7305963887313075084L;
			@Override
			public String call(String v1) throws Exception {
				return v1.toLowerCase();
			}
		});
		System.out.println("hword count:"+nonWordSplit.distinct().count());//130126
		System.out.println("100 hword:"+StringUtils.join(nonWordSplit.sample(true, 0.3, 42).take(100), ","));
		JavaRDD<String> filterNumbers = nonWordSplit.filter(new Function<String, Boolean>(){
			private static final long serialVersionUID = 7605510039216974581L;
			@Override
			public Boolean call(String v1) throws Exception {
				return v1.matches("([^0-9]*)");//过滤数字
			}
		});
		System.out.println("fword count:"+filterNumbers.distinct().count());//84912
		System.out.println("filter Num:"+StringUtils.join(filterNumbers.sample(true, 0.3, 42).take(100), ","));
		JavaPairRDD<String, Long> tokenCounts = filterNumbers.mapToPair(new PairFunction<String, String, Long>() {
			private static final long serialVersionUID = 8623062348882325774L;
			@Override
			public Tuple2<String, Long> call(String n) throws Exception {
				return new Tuple2<String, Long>(n, 1l);
			}
		}).reduceByKey(new Function2<Long, Long, Long>(){
			private static final long serialVersionUID = -8543095048185772337L;
			@Override
			public Long call(Long n1, Long n2) throws Exception {
				return n1+n2;
			}
		});//词组统计
		System.out.println("token count:"+tokenCounts.count());
		System.out.println("token reduce:"+tokenCounts.top(20, new SparkComparator<Tuple2<String, Long>>(){
			private static final long serialVersionUID = 8325517360060494043L;
			@Override
			public int compare(Tuple2<String, Long> v1, Tuple2<String, Long> v2) {
				return v2._2().intValue()-v1._2().intValue();
			}
		}));
		HashSet<String> stopwords = new HashSet<String>(Arrays.asList(new String[]{"the","a","an","of","or","in","for","by","on","but", "is", "not","with", "as", "was", "if","they", "are", "this", "and", "it", "have", "from", "at", "my","be", "that", "to"}));
		JavaPairRDD<String, Long> tokenCountsFilteredStopwords = tokenCounts.filter(new Function<Tuple2<String,Long>, Boolean>(){
			private static final long serialVersionUID = -4633359331982031363L;
			@Override
			public Boolean call(Tuple2<String,Long> v) throws Exception {
				return !stopwords.contains(v._1());
			}
		});//过滤停用词
		System.out.println("stop count:"+tokenCountsFilteredStopwords.count());
		System.out.println("filter stop:"+JSON.toJSONString(tokenCountsFilteredStopwords.top(20, new SparkComparator<Tuple2<String, Long>>(){
			private static final long serialVersionUID = -6989314110740313697L;
			@Override
			public int compare(Tuple2<String, Long> v1, Tuple2<String, Long> v2) {
				return v2._2().intValue()-v1._2().intValue();
			}
		})));
		JavaPairRDD<String, Long> tokenCountsFilteredSize = tokenCountsFilteredStopwords.filter(new Function<Tuple2<String,Long>, Boolean>(){
			private static final long serialVersionUID = -277762926206157798L;
			@Override
			public Boolean call(Tuple2<String,Long> v) throws Exception {
				return v._1().length()>=2;
			}
		});//过滤低频词
		System.out.println("size count:"+tokenCountsFilteredSize.count());
		System.out.println("filter size:"+tokenCountsFilteredSize.top(20,new SparkComparator<Tuple2<String, Long>>(){
			private static final long serialVersionUID = -5577692607801145895L;
			@Override
			public int compare(Tuple2<String, Long> v1, Tuple2<String, Long> v2) {
				return v1._2().intValue()-v2._2().intValue();
			}
		}));
		//从词组统计中获取低频词
		List<String> rareTokens = tokenCounts.filter(new Function<Tuple2<String,Long>, Boolean>(){
			private static final long serialVersionUID = 2752957636251643182L;
			@Override
			public Boolean call(Tuple2<String,Long> v) throws Exception {
				return v._2()<2;
			}
		}).map(new Function<Tuple2<String,Long>, String>(){
			private static final long serialVersionUID = -4339831411083874013L;
			@Override
			public String call(Tuple2<String, Long> v1) throws Exception {
				return v1._1();
			}
		}).collect();
		
		JavaPairRDD<String, Long> tokenCountsFilteredAll = tokenCountsFilteredSize.filter(new Function<Tuple2<String,Long>, Boolean>(){
			private static final long serialVersionUID = 2569014326714526208L;
			@Override
			public Boolean call(Tuple2<String,Long> v) throws Exception {
				return !rareTokens.contains(v._1());//过滤低频词
			}
		});
		System.out.println("filter 2+ count:"+tokenCountsFilteredAll.count());
		System.out.println("filter 2+ size:"+tokenCountsFilteredAll.top(20,new SparkComparator<Tuple2<String, Long>>(){
			private static final long serialVersionUID = 5164357512692190684L;
			@Override
			public int compare(Tuple2<String, Long> v1, Tuple2<String, Long> v2) {
				return v1._2().intValue()-v2._2().intValue();
			}
		}));
		JavaRDD<String> _tokens = text.flatMap(new FlatMapFunction<String, String>(){
			private static final long serialVersionUID = 3116471793344563385L;
			@Override
			public Iterator<String> call(String doc) throws Exception {
				return tokenize(doc, rareTokens);
			}
		});
		System.out.println(_tokens.distinct().count());
		JavaRDD<List<String>> tokens = text.map(new Function<String, List<String>>(){
			private static final long serialVersionUID = 4890841569799833519L;
			@SuppressWarnings("unchecked")
			@Override
			public List<String> call(String doc) throws Exception {
				return IteratorUtils.toList(tokenize(doc, rareTokens));
			}
		});
		System.out.println(tokens.first().subList(0, 20));
		int dim = Double.valueOf(Math.pow(2, 18)).intValue();
		HashingTF hashingTF = new HashingTF(dim);
		JavaRDD<Vector> tf = hashingTF.transform(tokens);
		tf.cache();
		SparseVector v = tf.first().toSparse();
		System.out.println("v size:"+v.size());
		System.out.println("v length:"+v.values().length);
		System.out.println(JSON.parseArray(JSON.toJSONString(v.values()), Double.class).subList(0, 10));
		System.out.println(JSON.parseArray(JSON.toJSONString(v.indices()), Integer.class).subList(0, 10));
		IDFModel idf = new IDF().fit(tf);
		JavaRDD<Vector> tfidf = idf.transform(tf);
		SparseVector v2 = tfidf.first().toSparse();
		System.out.println("v2 size:"+v2.size());
		System.out.println("v2 length:"+v2.values().length);
		System.out.println(JSON.parseArray(JSON.toJSONString(v2.values()), Double.class).subList(0, 10));
		System.out.println(JSON.parseArray(JSON.toJSONString(v2.indices()), Integer.class).subList(0, 10));
		JavaRDD<Tuple2<Double,Double>> minMaxVals = tfidf.map(new Function<Vector, Tuple2<Double, Double>>(){
			private static final long serialVersionUID = 2103717070465602739L;
			@Override
			public Tuple2<Double, Double> call(Vector vector) throws Exception {
				double[] values = vector.toSparse().values();
				double min = values[0], max = values[0];
				for (double value : values) {
					min = Math.min(min, value);
					max = Math.max(max, value);
				}
				return new Tuple2<Double, Double>(min, max);
			}
		});
		Tuple2<Double, Double> globalMinMax = minMaxVals.reduce(new Function2<Tuple2<Double, Double>, Tuple2<Double, Double>, Tuple2<Double, Double>>(){
			private static final long serialVersionUID = -4177846025351020160L;
			@Override
			public Tuple2<Double, Double> call(Tuple2<Double, Double> m1, Tuple2<Double, Double> m2) throws Exception {
				return new Tuple2<Double, Double>(Math.min(m1._1(), m2._1()), Math.max(m1._2(), m2._2()));
			}
		});
		System.out.println(globalMinMax);
		JavaRDD<String> common = sc.parallelize(Arrays.asList(new String[]{"you","do","we"}));
		JavaRDD<Vector> tfCommon = hashingTF.transform(common.map(new Function<String, List<String>>(){
			private static final long serialVersionUID = -128433598726261500L;
			@Override
			public List<String> call(String str) throws Exception {
				return Arrays.asList(str);
			}
		}));
		JavaRDD<Vector> tfidfCommon = idf.transform(tfCommon);
		SparseVector commonVector = tfidfCommon.first().toSparse();
		System.out.println(JSON.toJSONString(commonVector.values()));
		JavaPairRDD<String, String> hockeyText = data.filter(new Function<Tuple2<String,String>, Boolean>() {
			private static final long serialVersionUID = -6290848611283037688L;
			@Override
			public Boolean call(Tuple2<String, String> v1) throws Exception {
				String file = v1._1();
				return file.contains("hockey");
			}
		});
		JavaPairRDD<String, Vector> hockeyTF = hockeyText.mapValues(new Function<String, Vector>() {
			private static final long serialVersionUID = 3879484320084354111L;
			@Override
			public Vector call(String doc) throws Exception {
				return hashingTF.transform(IteratorUtils.toList(tokenize(doc, rareTokens)));
			}
		});
		JavaRDD<Vector> hockeyTfIdf = idf.transform(hockeyTF.map(new Function<Tuple2<String,Vector>, Vector>() {
			private static final long serialVersionUID = 4047902193225147740L;
			@Override
			public Vector call(Tuple2<String, Vector> v1) throws Exception {
				return v1._2();
			}
		}));
		SparseVector hockey1 = hockeyTfIdf.sample(true, 0.1, 42).first().toSparse();
		SparseVector breeze1 = new SparseVector(hockey1.size(),hockey1.indices(), hockey1.values());
		SparseVector hockey2 = hockeyTfIdf.sample(true, 0.1, 43).first().toSparse();
		SparseVector breeze2 = new SparseVector(hockey2.size(),hockey2.indices(), hockey2.values());
//		double cosineSim = breeze1.dot(breeze2) / (norm(breeze1) *norm(breeze2));
		double cosineSim = cosineSimilarity(new DoubleMatrix(breeze1.values()), new DoubleMatrix(breeze2.values()));
		System.out.println("cosineSim:"+cosineSim);
		JavaPairRDD<String, String> graphicsText = data.filter(new Function<Tuple2<String,String>, Boolean>() {
			private static final long serialVersionUID = 1061123715028270775L;
			@Override
			public Boolean call(Tuple2<String, String> v1) throws Exception {
				String file = v1._1();
				return file.contains("comp.graphics");
			}
		});
		JavaPairRDD<String, Vector> graphicsTF = graphicsText.mapValues(new Function<String, Vector>() {
			private static final long serialVersionUID = -4828647683441747819L;
			@Override
			public Vector call(String doc) throws Exception {
				return hashingTF.transform(IteratorUtils.toList(tokenize(doc, rareTokens)));
			}
		});
		JavaRDD<Vector> graphicsTfIdf = idf.transform(graphicsTF.map(new Function<Tuple2<String,Vector>, Vector>() {
			private static final long serialVersionUID = -1250947830931036060L;
			@Override
			public Vector call(Tuple2<String, Vector> v1) throws Exception {
				return v1._2();
			}
		}));
		SparseVector graphics = graphicsTfIdf.sample(true, 0.1, 42).first().toSparse();
		SparseVector breezeGraphics = new SparseVector(graphics.size(),graphics.indices(), graphics.values());
		/*double cosineSim2 = cosineSimilarity(new DoubleMatrix(breeze1.values()), new DoubleMatrix(breezeGraphics.values()));
		System.out.println("cosineSim2:"+cosineSim2);
		JavaPairRDD<String, String> baseballText = data.filter(new Function<Tuple2<String,String>, Boolean>() {
			private static final long serialVersionUID = -1047860510557051904L;
			@Override
			public Boolean call(Tuple2<String, String> v1) throws Exception {
				String file = v1._1();
				return file.contains("baseball");
			}
		});
		JavaPairRDD<String, Vector> baseballTF = baseballText.mapValues(new Function<String, Vector>() {
			private static final long serialVersionUID = -8500641348780172264L;
			@Override
			public Vector call(String doc) throws Exception {
				return hashingTF.transform(IteratorUtils.toList(tokenize(doc, rareTokens)));
			}
		});
		JavaRDD<Vector> baseballTfIdf = idf.transform(baseballTF.map(new Function<Tuple2<String,Vector>, Vector>() {
			private static final long serialVersionUID = -7036635722938704674L;
			@Override
			public Vector call(Tuple2<String, Vector> v1) throws Exception {
				return v1._2();
			}
		}));
		SparseVector baseball = baseballTfIdf.sample(true, 0.1, 42).first().toSparse();
		SparseVector breezeBaseball = new SparseVector(baseball.size(),baseball.indices(), baseball.values());
		double cosineSim3 = cosineSimilarity(new DoubleMatrix(breeze1.values()), new DoubleMatrix(breezeBaseball.values()));
		System.out.println("cosineSim3:"+cosineSim3);*/
		
		Map<String, Long> newsgroupsMap = newsgroups.distinct().zipWithIndex().collectAsMap();
		JavaPairRDD<String, Vector> zipped = newsgroups.zip(tfidf);
		JavaRDD<LabeledPoint> train = zipped.map(new Function<Tuple2<String,Vector>, LabeledPoint>() {
			private static final long serialVersionUID = 1588552979496600717L;
			@Override
			public LabeledPoint call(Tuple2<String, Vector> v1) throws Exception {
				return new LabeledPoint(newsgroupsMap.get(v1._1()), v1._2());
			}
		});
		double lambda = 0.1;
		NaiveBayesModel model = NaiveBayes.train(train.rdd(), lambda);//部署贝叶斯模型
		JavaPairRDD<String, String> tdata = sc.wholeTextFiles(path+"/test/*");
		JavaRDD<String> ttext = tdata.map(new Function<Tuple2<String,String>,String>(){
			private static final long serialVersionUID = 379354559473277480L;
			@Override
			public String call(Tuple2<String, String> v1) throws Exception {
				String text = v1._2();
				return text;
			}
		});
		ttext.cache();
		JavaRDD<String> testLabels = tdata.map(new Function<Tuple2<String,String>, String>() {
			private static final long serialVersionUID = -7134359392840333261L;
			@Override
			public String call(Tuple2<String, String> v1) throws Exception {
				String[] titles = v1._1().split("/"); 
				return titles[titles.length-2];
			}
		});
		JavaRDD<Vector> testTf = testLabels.map(new Function<String, Vector>() {
			private static final long serialVersionUID = -7431810830821268093L;
			@Override
			public Vector call(String doc) throws Exception {
				return hashingTF.transform(IteratorUtils.toList(tokenize(doc, rareTokens)));
			}
		});
		Map<String, Long> testLabelsMap = testLabels.distinct().zipWithIndex().collectAsMap();
		JavaRDD<Vector> testTfIdf = idf.transform(testTf);
		JavaPairRDD<String, Vector> zippedTest = testLabels.zip(testTfIdf);
		JavaRDD<LabeledPoint> test = zippedTest.map(new Function<Tuple2<String,Vector>, LabeledPoint>() {
			private static final long serialVersionUID = 214491387781791273L;
			@Override
			public LabeledPoint call(Tuple2<String, Vector> v1) throws Exception {
				return new LabeledPoint(testLabelsMap.get(v1._1()), v1._2());
			}
		});
		/*JavaPairRDD<Object, Object> predictionAndLabel = test.mapToPair(new PairFunction<LabeledPoint, Object, Object>() {
			private static final long serialVersionUID = 274929450468487736L;
			@Override
			public Tuple2<Object, Object> call(LabeledPoint p) throws Exception {
				Tuple2<Object, Object> value = new Tuple2<Object, Object>(model.predict(p.features()), p.label());
				return value;
			}
		});*/
		JavaRDD<Tuple2<Object, Object>> predictionAndLabel = test.map(new Function<LabeledPoint, Tuple2<Object, Object>>() {
			private static final long serialVersionUID = 2390541885706247113L;
			@Override
			public Tuple2<Object, Object> call(LabeledPoint p) throws Exception {
				Tuple2<Object, Object> value = new Tuple2<Object, Object>(model.predict(p.features()), p.label());
				return value;
			}
		});
		double accuracy = 1.0 *predictionAndLabel.filter(new Function<Tuple2<Object, Object>, Boolean>() {
			private static final long serialVersionUID = -3558833531740167211L;
			@Override
			public Boolean call(Tuple2<Object, Object> v1) throws Exception {
				return v1._1()==v1._2();
			}
		}).count()/test.count();
		MulticlassMetrics metrics = new MulticlassMetrics(predictionAndLabel.rdd());
		System.out.println("accuracy:"+accuracy);
		System.out.println("weightedFMeasure:"+metrics.weightedFMeasure());
		long seed = 42;
		Word2Vec word2vec = new Word2Vec();
		word2vec.setSeed(seed);
		Word2VecModel word2vecModel = word2vec.fit(tokens);
		Tuple2<String, Object>[] hockeyw2v = word2vecModel.findSynonyms("hockey", 20);
		for (Tuple2<String, Object> tuple2 : hockeyw2v) {
			System.out.println("("+tuple2._1()+","+tuple2._2()+")");
		}
		System.out.println("------------------------------------------------高级文本处理技术[end]------------------------------------------------");
		long end = System.currentTimeMillis();
		double time = (end - start) / 1000.00;
		double ss = time % 60;
		int mm = Double.valueOf(time / 60).intValue() % 60;
		int hh = Double.valueOf(time / 60 / 60).intValue() % 60;
		System.out.println("-------------------------regression耗时:"+(hh>0?hh+"小时":"")+(mm>0?mm+"分钟":"")+ss+"秒"+"-------------------------");
		sc.close();
		System.exit(1);
	}

	/**
	 * @decription 分词过滤(过滤[数字|停用词|低频字符串])
	 * @author yi.zhang
	 * @time 2017年7月12日 下午4:39:36
	 * @param line		目标词
	 * @param rareTokens低频词集合
	 * @return
	 */
	public static Iterator<String> tokenize(String line,List<String> rareTokens){
		// 停用词
		HashSet<String> stopwords = new HashSet<String>(Arrays.asList(new String[]{"the","a","an","of","or","in","for","by","on","but", "is", "not","with", "as", "was", "if","they", "are", "this", "and", "it", "have", "from", "at", "my","be", "that", "to"}));
		List<String> data = Arrays.asList(line.split("(\\W+)"));
		Stream<String> fdata = data.stream().map(t->t.toLowerCase())
		.filter(token->token.matches("([^0-9]*)"))
		.filter(token->!stopwords.contains(token))
		.filter(token->rareTokens!=null?!rareTokens.contains(token):true)
		.filter(token->token.length()>=2);
		return fdata.iterator();
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
			@Override
			public Iterator<String> call(String t) throws Exception {
				return Arrays.asList(t.split("(\\W+)")).iterator();//过滤符号
			}
		}).map(new Function<String,String>(){
			private static final long serialVersionUID = 7305963887313075084L;
			@Override
			public String call(String v1) throws Exception {
				return v1.toLowerCase();
			}
		}).filter(new Function<String, Boolean>(){
			private static final long serialVersionUID = 7605510039216974581L;
			@Override
			public Boolean call(String v1) throws Exception {
				return v1.matches("([^0-9]*)");//过滤数字
			}
		}).mapToPair(new PairFunction<String, String, Long>() {
			private static final long serialVersionUID = 8623062348882325774L;
			@Override
			public Tuple2<String, Long> call(String n) throws Exception {
				return new Tuple2<String, Long>(n, 1l);
			}
		}).reduceByKey(new Function2<Long, Long, Long>(){
			private static final long serialVersionUID = -8543095048185772337L;
			@Override
			public Long call(Long n1, Long n2) throws Exception {
				return n1+n2;
			}
		});//词组统计
		//从词组统计中获取低频词
		List<String> rareTokens = tokenCounts.filter(new Function<Tuple2<String,Long>, Boolean>(){
			private static final long serialVersionUID = 2752957636251643182L;
			@Override
			public Boolean call(Tuple2<String,Long> v) throws Exception {
				return v._2()<2;
			}
		}).map(new Function<Tuple2<String,Long>, String>(){
			private static final long serialVersionUID = -4339831411083874013L;
			@Override
			public String call(Tuple2<String, Long> v1) throws Exception {
				return v1._1();
			}
		}).collect();
		
		HashSet<String> stopwords = new HashSet<String>(Arrays.asList(new String[]{"the","a","an","of","or","in","for","by","on","but", "is", "not","with", "as", "was", "if","they", "are", "this", "and", "it", "have", "from", "at", "my","be", "that", "to"}));
		JavaRDD<String> tokenFilter = tokenCounts.filter(new Function<Tuple2<String,Long>, Boolean>(){
			private static final long serialVersionUID = -4633359331982031363L;
			@Override
			public Boolean call(Tuple2<String,Long> v) throws Exception {
				return !stopwords.contains(v._1());//过滤停用词
			}
		}).filter(new Function<Tuple2<String,Long>, Boolean>(){
			private static final long serialVersionUID = -277762926206157798L;
			@Override
			public Boolean call(Tuple2<String,Long> v) throws Exception {
				return v._1().length()>=2;//过滤低频词
			}
		}).filter(new Function<Tuple2<String,Long>, Boolean>(){
			private static final long serialVersionUID = 2569014326714526208L;
			@Override
			public Boolean call(Tuple2<String,Long> v) throws Exception {
				return !rareTokens.contains(v._1());//过滤低频词
			}
		}).map(new Function<Tuple2<String,Long>, String>() {
			private static final long serialVersionUID = 65429930914004853L;
			@Override
			public String call(Tuple2<String, Long> v1) throws Exception {
				return v1._1();
			}
		});
		return tokenFilter;
	}
	/**
	 * @decription Spark Streaming应用
	 * @author yi.zhang
	 * @throws Exception 
	 * @time 2017年7月17日 上午9:58:50
	 */
	public void sstream() throws Exception{
		long start = System.currentTimeMillis();
		String master = "local[2]";// 主节点运行模式: local | yarn-client | yarn-standalone
		String appName = "SparkMLTest";// 应用名称
		String spark_home = System.getenv("SPARK_HOME");
//		String path = System.getProperty("user.dir") + "/src/main/resources"+"/data/pu_stream";
//		online();
		System.out.println("-------------------------------------------------------------------"+spark_home);
		JavaSparkContext sc = new JavaSparkContext(master, appName);
		sc.setLogLevel("warn");
		sc.getConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
		sc.getConf().registerKryoClasses(new Class[] { JavaSerializer.class,KryoSerializer.class,GenericAvroSerializer.class });
		JavaStreamingContext ssc = new JavaStreamingContext(sc,Durations.seconds(10));
		JavaReceiverInputDStream<String> stream = ssc.socketTextStream("localhost", 9999);
		int flag = 3;
		if(flag==1){//generateProductEvents()
			// 简单地打印每一批的前几个元素
			// 批量运行
//			stream.print();
//			JavaDStream<Tuple3<String, String, Double>> events = stream.map(record->{
//				String[] event = record.split(",");
//				return new Tuple3<String, String, Double>(event[0], event[1],Double.valueOf(event[2]));
//			});
			JavaDStream<Tuple3<String, String, Double>> events = stream.map(record->new Tuple3<String, String, Double>(record.split(",")[0], record.split(",")[1],Double.valueOf(record.split(",")[2])));
//			JavaDStream<Tuple3<String, String, Double>> events = stream.map(new Function<String, Tuple3<String, String, Double>>() {
//				private static final long serialVersionUID = -5471480745861883618L;
//				@Override
//				public Tuple3<String, String, Double> call(String record) throws Exception {
//					String[] event = record.split(",");
//					Tuple3<String, String, Double> v1 = new Tuple3<String, String, Double>(event[0], event[1],Double.valueOf(event[2]));
//					return v1;
//				}
//			});
			events.foreachRDD((rdd, time)->{
				long numPurchases = rdd.count();
				if(numPurchases==0)return;
				long uniqueUsers = rdd.map(obj->obj._1()).distinct().count();
				Double totalRevenue = rdd.mapToDouble(obj->obj._3()).sum();
				List<Tuple2<String, Long>> productsByPopularity = rdd.mapToPair(obj->new Tuple2<String,Long>(obj._2(),1l)).reduceByKey((v1,v2)->v1+v2).collect();
				/*Collections.sort(productsByPopularity, new SparkComparator<Tuple2<String, Long>>() {
					private static final long serialVersionUID = -8581812469981971018L;
					@Override
					public int compare(Tuple2<String, Long> o1, Tuple2<String, Long> o2) {
						return o1._2().intValue()-o2._2().intValue();
					}
				});*/
//				productsByPopularity.sort((v1,v2)->v1._2().intValue()-v2._2().intValue());
				Tuple2<String, Long> mostPopular = productsByPopularity.get(0);
				String dateStr = DateUtil.formatDateTimeStr(new Date(time.milliseconds()));
				System.out.println("== Batch start time: "+dateStr+" ==");
				System.out.println("Total purchases: " + numPurchases);
				System.out.println("Unique users: " + uniqueUsers);
				System.out.println("Total revenue: " + totalRevenue);
				System.out.println(String.format("Most popular product: %s with %d purchases",mostPopular._1(), mostPopular._2()));
			});
			JavaDStream<Tuple2<String,Tuple2<String,Double>>> users = events.map(obj->new Tuple2<String,Tuple2<String,Double>>(obj._1(),new Tuple2<String,Double>(obj._2(),obj._3())));
		}
		if(flag==2){// generateNoisyData()
			int NumFeatures = 100;
//			DenseVector.zeros[Int](NumFeatures);
			org.apache.spark.mllib.linalg.DenseVector zeroVector = Vectors.zeros(NumFeatures).toDense();
			StreamingLinearRegressionWithSGD model = new StreamingLinearRegressionWithSGD().setInitialWeights(Vectors.dense(zeroVector.values())).setNumIterations(1).setStepSize(0.01);
			JavaDStream<LabeledPoint> labeledStream = stream.map(event->{
				String[] split = event.split("\t");
				double y = Double.valueOf(split[0]);
				double[] features = Arrays.asList(split[1].split(",")).stream().mapToDouble(d->Double.valueOf(d)).toArray();
				return new LabeledPoint(y, Vectors.dense(features));
			});
			// 在流上训练测试模型，并打印预测结果作为展示
			model.trainOn(labeledStream);
			model.predictOn(labeledStream.map(lp->lp.features())).print();
		}
		if(flag==3){// generateNoisyData()
			int NumFeatures = 100;
//			DenseVector.zeros[Int](NumFeatures);
			org.apache.spark.mllib.linalg.DenseVector zeroVector = Vectors.zeros(NumFeatures).toDense();
			StreamingLinearRegressionWithSGD model1 = new StreamingLinearRegressionWithSGD().setInitialWeights(Vectors.dense(zeroVector.values())).setNumIterations(1).setStepSize(0.01);
			StreamingLinearRegressionWithSGD model2 = new StreamingLinearRegressionWithSGD().setInitialWeights(Vectors.dense(zeroVector.values())).setNumIterations(1).setStepSize(1.0);
			JavaDStream<LabeledPoint> labeledStream = stream.map(event->{
				String[] split = event.split("\t");
				double y = Double.valueOf(split[0]);
				double[] features = Arrays.asList(split[1].split(",")).stream().mapToDouble(d->Double.valueOf(d)).toArray();
				return new LabeledPoint(y, Vectors.dense(features));
			});
			// 在流上训练测试模型，并打印预测结果作为展示
			model1.trainOn(labeledStream);
			model2.trainOn(labeledStream);
			JavaDStream<Tuple2<Double,Double>> predsAndTrue = labeledStream.transform(rdd->{
				LinearRegressionModel latest1 = model1.latestModel();
				LinearRegressionModel latest2 = model2.latestModel();
				JavaRDD<Tuple2<Double,Double>> preds = rdd.map(point->{
					double pred1 = latest1.predict(point.features());
					double pred2 = latest2.predict(point.features());
					return new Tuple2<Double,Double>(pred1 - point.label(), pred2 - point.label());
				});
				return preds;
			});
			predsAndTrue.foreachRDD((rdd, time)->{
				Double mse1 =  rdd.mapToDouble(obj->Math.pow(obj._1(), 2)).mean();
				double rmse1 = Math.sqrt(mse1);
				Double mse2 =  rdd.mapToDouble(obj->Math.pow(obj._2(), 2)).mean();
				double rmse2 = Math.sqrt(mse1);
				System.out.println(String.format("|-------------------------------------------\n|Time: %s\n|-------------------------------------------", time));
				System.out.println("MSE current batch: Model 1: "+mse1+"; Model 2:"+mse2);
				System.out.println("RMSE current batch: Model 1: "+rmse1+"; Model 2:"+rmse2);
			});
		}
		
		ssc.start();
		ssc.awaitTermination();	
		System.out.println("------------------------------------------------Spark Streaming应用[start]------------------------------------------------");
		System.out.println("------------------------------------------------Spark Streaming应用[end]------------------------------------------------");
		long end = System.currentTimeMillis();
		double time = (end - start) / 1000.00;
		double ss = time % 60;
		int mm = Double.valueOf(time / 60).intValue() % 60;
		int hh = Double.valueOf(time / 60 / 60).intValue() % 60;
		System.out.println("-------------------------regression耗时:"+(hh>0?hh+"小时":"")+(mm>0?mm+"分钟":"")+ss+"秒"+"-------------------------");
		ssc.close();
		sc.close();
		System.exit(1);
	}
	
	public static void generateProductEvents() throws Exception{
		String path = System.getProperty("user.dir") + "/src/main/resources" + "/data/pu_stream";
		@SuppressWarnings("resource")
		ServerSocket listener = new ServerSocket(9999);
		System.out.println("Listening on port: 9999");
		while (true) {
			Socket socket = listener.accept();
			new Thread(new Runnable() {
				@Override
				public void run() {
					try {
						System.out.println("Got client connected from: " + socket.getInetAddress());
						PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
						while (true) {
							Thread.sleep(1000);
							Random random = new Random();
							int MaxEvents = 6;// 每秒最大活动数
							String[] names = scala.io.Source.fromFile(path + "/names.csv", "UTF-8").getLines().toList().head().split(",");
							System.out.println(JSON.toJSONString(names));
							List<Tuple2<String, Double>> products = new ArrayList<Tuple2<String, Double>>();
							products.add(new Tuple2<String, Double>("iPhone Cover", 9.99));
							products.add(new Tuple2<String, Double>("Headphones", 5.49));
							products.add(new Tuple2<String, Double>("Samsung Galaxy Cover", 8.95));
							products.add(new Tuple2<String, Double>("iPad Cover", 7.49));
							List<Tuple3<String, String, Double>> productEvents = new ArrayList<Tuple3<String, String, Double>>();
							int num = random.nextInt(MaxEvents);
							for (int i = 1; i <= num; i++) {
								Tuple2<String, Double> obj = products.get(random.nextInt(products.size()));
								// String user = random.shuffle(names).head();
								String user = names[random.nextInt(names.length)];
								String product = obj._1();
								double price = obj._2();
								Tuple3<String, String, Double> v1 = new Tuple3<String, String, Double>(user, product,
										price);
								productEvents.add(v1);
							}
							productEvents.forEach(event -> {
								out.write(event._1() + "," + event._2() + "," + event._3());
								out.write("\n");
							});
							out.flush();
							System.out.println("Created $num events...");
						}
//						socket.close();
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}).start();
		}
	}
	public static void generateNoisyData() throws Exception{
		@SuppressWarnings("resource")
		ServerSocket listener = new ServerSocket(9999);
		System.out.println("Listening on port: 9999");
		while (true) {
			Socket socket = listener.accept();
			new Thread(new Runnable() {
				@Override
				public void run() {
					try {
						System.out.println("Got client connected from: " + socket.getInetAddress());
						PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
						while (true) {
							Thread.sleep(1000);
							Random random = new Random();
							int MaxEvents = 100;
							int NumFeatures = 100;
//							Array<T>.tabulate(NumFeatures, arg1, arg2);
							double[] generateRandomArray= new double[NumFeatures];
							for(int i=0;i<NumFeatures;i++)generateRandomArray[i]=random.nextGaussian();
							DenseVector<Double> w = new DenseVector<Double>(generateRandomArray);
							double intercept = random.nextGaussian() * 10;
							int num = random.nextInt(MaxEvents);
							List<Tuple2<Double, DenseVector<Double>>> data = new ArrayList<Tuple2<Double, DenseVector<Double>>>();
							for(int i=1;i<num;i++){
								DenseVector<Double> x = new DenseVector<Double>(generateRandomArray);
								Double y= w.dot(x,null);//参数2无法确定
								Double noisy = y + intercept;
								data.add(new Tuple2<Double, DenseVector<Double>>(noisy,x));
							}
							data.forEach(event -> {
								out.write(event._1() + "\t" + event._2().valuesIterator().mkString(","));
								out.write("\n");
							});
							out.flush();
							System.out.println("Created $num events...");
						}
//						socket.close();
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}).start();
		}
	}
	
	public static void nosql(){
		String master = "local[2]";// 主节点运行模式: local | yarn-client | yarn-standalone
		String appName = "SparkMLTest";// 应用名称
		JavaSparkContext sc = new JavaSparkContext(master, appName);
//		sc.setLogLevel("info");
		sc.getConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
		sc.getConf().registerKryoClasses(new Class[] { JavaSerializer.class,KryoSerializer.class,GenericAvroSerializer.class });
		//cassandra
		sc.getConf().set("spark.cassandra.connection.host", "127.0.0.1,192.168.0.1");
		sc.getConf().set("spark.cassandra.connection.port", "9042");
		sc.getConf().set("spark.cassandra.auth.username", "cassandra"); // optional
		sc.getConf().set("spark.cassandra.auth.password", "cassandra"); // optional
		CassandraJavaUtil.javaFunctions(sc).cassandraTable("test", "table", CassandraJavaUtil.mapRowTo(User.class)).select(
				CassandraJavaUtil.column("no_alias"),
				CassandraJavaUtil.column("simple").as("simpleProp"),
				CassandraJavaUtil.ttl("simple").as("simplePropTTL"),
				CassandraJavaUtil.writeTime("simple").as("simpleWriteTime"));
		
		//mongodb
		String url = "mongodb://username:password@localhost:27017/test.coll";
		sc.getConf().set("spark.mongodb.input.uri", url);
		sc.getConf().set("spark.mongodb.output.uri", url);
		JavaMongoRDD<Document> mdata = MongoSpark.load(sc);
		
		SparkSession spark = SparkSession.builder()
			      .master("local")
			      .appName("MongoSparkConnectorIntro")
			      .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/test.myCollection")
			      .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/test.myCollection")
			      .getOrCreate();

			    // Create a JavaSparkContext using the SparkSession's SparkContext object
			    JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

			    // Load data and infer schema, disregard toDF() name as it returns Dataset
			    Dataset<Row> implicitDS = MongoSpark.load(jsc).toDF();
			    implicitDS.printSchema();
			    implicitDS.show();

			    // Load data with explicit schema
			    Dataset<User> explicitDS = MongoSpark.load(jsc).toDS(User.class);
			    explicitDS.printSchema();
			    explicitDS.show();

			    // Create the temp view and execute the query
			    explicitDS.createOrReplaceTempView("users");
			    Dataset<Row> centenarians = spark.sql("SELECT name, age FROM users WHERE age >= 100");
			    centenarians.show();

			    // Write the data to the "hundredClub" collection
			    MongoSpark.write(centenarians).option("collection", "hundredClub").mode("overwrite").save();

			    // Load the data from the "hundredClub" collection
			    MongoSpark.load(jsc, ReadConfig.create(jsc).withOption("collection", "hundredClub"), Character.class);

			    jsc.close();
	}
	public static void main(String[] args) throws Exception {
		long start = System.currentTimeMillis();
		System.out.println("======================================================");
		SparkMLTest test = new SparkMLTest();
//		k_1();
//		test.recommend();
		test.classify();
//		test.regression();
		test.cluster();
//		test.dimension();
//		test.htext1();
		
		
//		generateProductEvents();
//		generateNoisyData();
//		test.sstream();
		long end = System.currentTimeMillis();
		double time = (end-start)/1000.00;
		double ss = time%60;
		int mm = Double.valueOf(time/60).intValue()%60;
		int hh = Double.valueOf(time/60/60).intValue()%60;
		System.out.println("-------------------------main耗时:"+(hh>0?hh+"小时":"")+(mm>0?mm+"分钟":"")+ss+"秒"+"-------------------------");
	}

}