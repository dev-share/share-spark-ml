package com.glocalme.share.spark.vsim.sleep;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.IteratorUtils;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.DoubleFunction;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.tree.DecisionTree;
import org.apache.spark.mllib.tree.model.DecisionTreeModel;
import org.apache.spark.serializer.GenericAvroSerializer;
import org.apache.spark.serializer.JavaSerializer;
import org.apache.spark.serializer.KryoSerializer;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.SparkSession.Builder;
import org.bson.Document;
import org.bson.types.ObjectId;

import com.alibaba.fastjson.JSON;
import com.glocalme.share.spark.common.SparkComparator;
import com.glocalme.share.spark.common.SparkConfig;
import com.glocalme.share.spark.util.StringUtil;
import com.glocalme.share.spark.vsim.pojo.TerminalFlowUpload;
import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;

import scala.Tuple2;
/**
 * @decription 卡休眠机器学习
 * @author yi.zhang
 * @time 2017年8月3日 上午11:52:34
 * @since 1.0
 * @jdk	1.8
 */
public class VsimSleepML implements Serializable {
	private static final long serialVersionUID = 1135129402205052489L;
	public JavaRDD<TerminalFlowUpload> mongodb(){
		String master = SparkConfig.getProperty("spark.ml.master");// 主节点运行模式: local | yarn-client | yarn-standalone
		String appName = this.getClass().getSimpleName();// 应用名称
		String iurl = SparkConfig.getProperty("spark.mongodb.input.uri");
		String idatabase = SparkConfig.getProperty("spark.mongodb.input.database");
		String ischema = SparkConfig.getProperty("spark.mongodb.input.schema");
		String ourl = SparkConfig.getProperty("spark.mongodb.output.uri");
		String odatabase = SparkConfig.getProperty("spark.mongodb.output.database");
		String oschema = SparkConfig.getProperty("spark.mongodb.output.schema");
//		String itable = "t_terminal_flow_upload_forML";
		String itable = "share_terminal_flow_upload";
		String otable = "share_terminal_flow_upload_ml";
//		String auth = "mongodb://username:password@127.0.0.1:27017/db.table?authSource=admin&readPreference=primaryPreferred";
		String rurl = iurl+"/"+ischema+"."+itable+"?authSource="+idatabase+"";
		String wurl = ourl+"/"+oschema+"."+otable+"?authSource="+odatabase+"&readPreference=primaryPreferred";
		Builder builder = SparkSession.builder();
		builder.master(master);
		builder.appName(appName);
//		builder.config("mongo.auth.uri", auth);
		builder.config("spark.mongodb.keep_alive_ms", 24*60*60*1000);
		builder.config("spark.mongodb.input.sampleSize", 10000);
		builder.config("spark.mongodb.input.readPreference.name", "primaryPreferred");//primary|primaryPreferred|secondary|secondaryPreferred|nearest
//		builder.config("spark.mongodb.input.partitioner", "MongoShardedPartitioner");//MongoDefaultPartitioner|MongoSamplePartitioner|MongoShardedPartitioner|MongoSplitVectorPartitioner|MongoPaginateByCountPartitioner|MongoPaginateBySizePartitioner
//		builder.config("spark.mongodb.input.partitionerOptions.samplesPerPartition", 10000);
//		builder.config("spark.mongodb.input.database", schema);
//		builder.config("spark.mongodb.input.collection", table);
		builder.config("spark.mongodb.input.uri", rurl);
		builder.config("spark.mongodb.output.uri", wurl);
//		builder.config("spark.mongodb.output.maxBatchSize", 10000);
		SparkSession session = builder.getOrCreate();
		SparkContext spark = session.sparkContext();
		spark.getConf().set("spark.serializer", "org.apache.spark.serializer.JavaSerializer");
		spark.getConf().set("spark.driver.memory", "2G");
		spark.getConf().set("spark.executor.memory", "2G");
		spark.getConf().set("spark.driver.cores", "2");
		spark.getConf().set("spark.executor.cores", "2");
		spark.getConf().set("spark.driver.maxResultSize", "0");//默认1g,最小1M,0无限制
		spark.getConf().registerKryoClasses(new Class[] { JavaSerializer.class,KryoSerializer.class,GenericAvroSerializer.class });
		JavaSparkContext jsc = new JavaSparkContext(spark);
		JavaMongoRDD<Document> mdata = MongoSpark.load(jsc);
//		long start = DateUtil.formatDate("2017-07-01").getTime();
//		long end = DateUtil.formatDateTime("2017-07-05 23:59:59").getTime();
//		List<Document> params = new ArrayList<Document>();
//		params.add(Document.parse("{ $match: { beginTime : { $gt : "+start+", $lt : "+end+"} } }"));
//		params.add(Document.parse("{ $project: {userCode : 1,imei : 1,imsi : 1,beginTime : 1,user : 1,mcc : 1 } }"));
//		JavaMongoRDD<Document> mdata = MongoSpark.load(jsc).withPipeline(params);
		JavaRDD<TerminalFlowUpload> data = mdata.filter(new Function<Document, Boolean>() {
			private static final long serialVersionUID = -8427927133613120516L;
			public Boolean call(Document v1) throws Exception {
				Object bvalue = v1.get("beginTime");
				return bvalue!=null;
			}
		}).map(new Function<Document, TerminalFlowUpload>() {
			private static final long serialVersionUID = -5773613499725254316L;
			public TerminalFlowUpload call(Document v1) throws Exception {
				Object id = v1.get("_id");
				if(id instanceof ObjectId){
					v1.put("_id", v1.getObjectId("_id").toHexString());
				}
				String json = JSON.toJSONString(v1);
				if(json.contains("{}")){
					for(Field field:TerminalFlowUpload.class.getDeclaredFields()){
						String tfield = field.getName();
						Object value = v1.get(tfield);
						if(!(value instanceof Integer||value instanceof Long||value instanceof Double||value instanceof String)){
							v1.put(tfield, null);
						}
					}
					json = JSON.toJSONString(v1);
				}
				TerminalFlowUpload obj = JSON.parseObject(json, TerminalFlowUpload.class);
				if(StringUtil.isEmptyStr(obj.getLatitude()))obj.setLatitude("0");
				if(StringUtil.isEmptyStr(obj.getLongitude()))obj.setLongitude("0");
				if(StringUtil.isEmptyStr(obj.getMcc()))obj.setMcc("0");
				return obj;
			}
		});
		return data;
	}
	public String ml(final long boundary,final double relative,final int maxDepth,final int numClasses,final int maxBins){
		JavaRDD<TerminalFlowUpload> data = mongodb();
//		data.cache();
		final Map<String, Long> userCodes = data.map(obj->obj.getUserCode()).zipWithIndex().collectAsMap();
//		final Map<String, Long> sessionids = data.map(obj->obj.getSessionid()).zipWithIndex().collectAsMap();
//		final Map<String, Long> uids = data.map(obj->obj.getUid()).zipWithIndex().collectAsMap();
//		final Map<String, Long> countrys = data.map(obj->obj.getCountry()).zipWithIndex().collectAsMap();
		JavaPairRDD<String, TerminalFlowUpload> kdata = data.mapToPair(new PairFunction<TerminalFlowUpload, String, TerminalFlowUpload>() {
			private static final long serialVersionUID = -5751805103271881338L;
			public Tuple2<String, TerminalFlowUpload> call(TerminalFlowUpload obj) throws Exception {
				String key = obj.getUserCode()+"_"+obj.getImsi()+"_"+obj.getImei();
				return new Tuple2<String, TerminalFlowUpload>(key,obj);
			}
		});
		kdata = kdata.groupByKey().map(new Function<Tuple2<String,Iterable<TerminalFlowUpload>>, List<TerminalFlowUpload>>() {
			private static final long serialVersionUID = 3976043732150570800L;
			@Override
			public List<TerminalFlowUpload> call(Tuple2<String, Iterable<TerminalFlowUpload>> v1)throws Exception {
				Iterable<TerminalFlowUpload> iterable = v1._2();
				@SuppressWarnings("unchecked")
				List<TerminalFlowUpload> list = IteratorUtils.toList(iterable.iterator());
				Collections.sort(list, new SparkComparator<TerminalFlowUpload>() {
					private static final long serialVersionUID = 1752618111301298050L;
					@Override
					public int compare(TerminalFlowUpload o1, TerminalFlowUpload o2) {
						if(o1.getBeginTime()==o2.getBeginTime()){
							return 0;
						}else{
							if(o1.getBeginTime()>o2.getBeginTime()){
								return 1;
							}else{
								return -1;
							}
						}
					}
				});
				for(int i=0;i<list.size();i++){
					TerminalFlowUpload obj = list.get(i);
					if(i==0||i==1){
						obj.setUser1(obj.getUser());
						obj.setUser2(obj.getUser());
						if(i==1){
							obj.setUser1(list.get(0).getUser());
							obj.setUser2(list.get(0).getUser());
						}
					}else{
						obj.setUser1(list.get(i-1).getUser());
						obj.setUser2(list.get(i-2).getUser());
					}
				}
				return list;
			}
		}).flatMap(new FlatMapFunction<List<TerminalFlowUpload>, TerminalFlowUpload>() {
			private static final long serialVersionUID = -7920139020149262264L;
			@Override
			public Iterator<TerminalFlowUpload> call(List<TerminalFlowUpload> list) throws Exception {
				return list.iterator();
			}
		}).mapToPair(new PairFunction<TerminalFlowUpload, String, TerminalFlowUpload>() {
			private static final long serialVersionUID = -3023533988978597497L;
			@Override
			public Tuple2<String, TerminalFlowUpload> call(TerminalFlowUpload obj) throws Exception {
				String key = obj.getUserCode()+"_"+obj.getImsi()+"_"+obj.getImei();
				return new Tuple2<String, TerminalFlowUpload>(key,obj);
			}
		});
//		kdata.cache();
		JavaRDD<Tuple2<TerminalFlowUpload, Vector>> cdata = kdata.map(new Function<Tuple2<String,TerminalFlowUpload>, Tuple2<TerminalFlowUpload,Vector>>() {
			private static final long serialVersionUID = -1119637548877107334L;
			@Override
			public Tuple2<TerminalFlowUpload, Vector> call(Tuple2<String, TerminalFlowUpload> v1) throws Exception {
				TerminalFlowUpload obj = v1._2();
//				String cellId = obj.getCellId();
//				Long cid = obj.getCid();
//				Integer devicetype = obj.getDevicetype();
//				String lac = obj.getLac();
//				String latitude = StringUtil.isEmptyStr(obj.getLatitude())?"0":obj.getLatitude();
//				String longitude = StringUtil.isEmptyStr(obj.getLongitude())?"0":obj.getLongitude();
//				String uid = obj.getUid();
//				String iso2 = obj.getIso2();
//				String logId = obj.getLogId();
//				String plmn = obj.getPlmn();
				String mcc  = obj.getMcc();
//				String country = obj.getCountry();
//				String sessionid = obj.getSessionid();
				String userCode = obj.getUserCode();
				Long imei = obj.getImei();
				String imsi = obj.getImsi();
				Long beginTime = obj.getBeginTime();
//				Long countDay = obj.getCountDay();
//				Integer card = obj.getCard();
//				Long cardDownFlow = obj.getCardDownFlow();
//				Long cardUpFlow = obj.getCardUpFlow();
//				Long sys = obj.getSys();
//				Long sysDownFlow = obj.getSysDownFlow();
//				Long sysUpFlow = obj.getSysUpFlow();
				Long user = obj.getUser();
//				Long userDownFlow = obj.getUserDownFlow();
//				Long userUpFlow = obj.getUserUpFlow();
//				System.out.println(JSON.toJSONString(obj));
//				double[] features = new double[]{userCodes.get(userCode),imei,Long.valueOf(imsi),beginTime,countDay,card,cardDownFlow,cardUpFlow,sys,sysDownFlow,sysUpFlow,user,userDownFlow,userUpFlow,sessionids.get(sessionid),uids.get(uid),cid,devicetype,Integer.valueOf(mcc),Integer.valueOf(cellId),Integer.valueOf(lac),Double.valueOf(latitude),Double.valueOf(longitude),countrys.get(country)};
				double[] features = new double[]{userCodes.get(userCode),imei,Long.valueOf(imsi),beginTime,user,obj.getUser1(),obj.getUser2(),Integer.valueOf(mcc)};
				return new Tuple2<TerminalFlowUpload, Vector>(obj,Vectors.dense(features));
			}
		});
//		System.out.println(data.count()+"-->"+kdata.count());//1280000
		JavaPairRDD<String, Long> sdata = kdata.mapToPair(new PairFunction<Tuple2<String,TerminalFlowUpload>, String, Long>() {
			private static final long serialVersionUID = -3845117773887204553L;
			public Tuple2<String, Long> call(Tuple2<String, TerminalFlowUpload> v1) throws Exception {//使用总流量=系统流量+用户流量
				String key = v1._1();
				TerminalFlowUpload obj = v1._2();
				Long value = obj.getUser();
				return new Tuple2<String, Long>(key,value);
			}
		}).reduceByKey(new Function2<Long, Long, Long>() {
			private static final long serialVersionUID = -4906373309161293262L;
			public Long call(Long v1, Long v2) throws Exception {//分组求和
				return v1+v2;
			}
		});
		final Map<String, Long> key_count = kdata.countByKey();//分组数量
		final Map<String,Double> key_avg = sdata.mapToPair(new PairFunction<Tuple2<String,Long>, String,Double>() {
			private static final long serialVersionUID = -7135157781412960366L;
			public Tuple2<String, Double> call(Tuple2<String, Long> v1) throws Exception {//求其分组平均值
				String key = v1._1();
				Long sum = v1._2();
				Long count = key_count.get(key);
				Double value = (double) (sum/count);
				return new Tuple2<String, Double>(key,value);
			}
		}).collectAsMap();
		
		long seed = 42;
		
//		JavaRDD<TerminalFlowUpload>[] hdata = data.randomSplit(new double[]{0.8,0.2}, seed);
//		JavaRDD<TerminalFlowUpload> sample_train = hdata[0];
//		JavaRDD<TerminalFlowUpload> sample_test = hdata[1];
		JavaRDD<Tuple2<TerminalFlowUpload, Vector>> train_sample = cdata.sample(false, 0.8, seed);
		JavaRDD<Tuple2<TerminalFlowUpload, Vector>> test_sample = cdata.subtract(train_sample);
//		final Long boundary = 100*1000l;
//		final double relative = 0.5;
		JavaRDD<Tuple2<TerminalFlowUpload, Vector>> train_data = train_sample.map(new Function<Tuple2<TerminalFlowUpload,Vector>, Tuple2<TerminalFlowUpload, Vector>>() {
			private static final long serialVersionUID = -4406434363228552977L;
			@Override
			public Tuple2<TerminalFlowUpload, Vector> call(Tuple2<TerminalFlowUpload, Vector> v1) throws Exception {
				TerminalFlowUpload obj = v1._1();
				String key = obj.getUserCode()+"_"+obj.getImsi()+"_"+obj.getImei();
				Double avg = key_avg.get(key);
				double rate = obj.getUser()/avg;
				double rate1 = obj.getUser1()/avg;
				double rate2 = obj.getUser2()/avg;
				if(obj.getUser()<boundary&&((obj.getUser1()<boundary&&obj.getUser2()<boundary)||(rate<relative&&rate1<relative&&rate2<relative))){
					obj.setSleep(1);//休眠	
				}else{
					obj.setSleep(0);//可休眠可不休眠
					if(obj.getUser()>boundary&&obj.getUser()>avg){
						obj.setSleep(2);//不休眠
					}
				}
				Vector features = v1._2();
				return new Tuple2<TerminalFlowUpload,Vector>(obj,features);
			}
		});
		JavaRDD<Tuple2<TerminalFlowUpload, Vector>> test_data = test_sample;
		JavaRDD<LabeledPoint> train = train_data.map(new Function<Tuple2<TerminalFlowUpload,Vector>, LabeledPoint>() {
			private static final long serialVersionUID = 8989258783993594619L;
			@Override
			public LabeledPoint call(Tuple2<TerminalFlowUpload,Vector> v1) throws Exception {
				TerminalFlowUpload obj=v1._1();
				int label = obj.getSleep();
				Vector features = v1._2();
				return new LabeledPoint(label,features);
			}
		});
		
//		LogisticRegressionModel lrModel = LogisticRegressionWithSGD.train(train.rdd(), numIterations);//逻辑回归模型
//		SVMModel svmModel = SVMWithSGD.train(train.rdd(), numIterations);//向量机(SVM)模型
//		NaiveBayesModel nbModel = NaiveBayes.train(train.rdd());//朴素贝叶斯模型
//		int maxDepth=10;
//		int numClasses=3;
//		int maxBins=32;//固定划分数
//		final DecisionTreeModel model = DecisionTree.train(train.rdd(), Algo.Classification(), Entropy.instance(), maxDepth);//决策树模型(Algo为算法[Classification,Regression],Impurity为不纯度估计[Entropy分类,Gini分类,Variance回归])
//		final DecisionTreeModel model = DecisionTree.train(train.rdd(), Algo.Classification(), Entropy.instance(), maxDepth,numClasses);//决策树模型(Algo为算法[Classification,Regression],Impurity为不纯度估计[Entropy分类,Gini分类,Variance回归])
		final DecisionTreeModel model = DecisionTree.trainClassifier(train, numClasses, new HashMap<Integer, Integer>(), "entropy", maxDepth, maxBins);
//		JavaPairRDD<TerminalFlowUpload, Integer> actual = train_data.mapToPair(new PairFunction<Tuple2<TerminalFlowUpload,Vector>, TerminalFlowUpload,Integer>() {
//			private static final long serialVersionUID = 790041945509045427L;
//			@Override
//			public Tuple2<TerminalFlowUpload,Integer> call(Tuple2<TerminalFlowUpload,Vector> v1) throws Exception {
//				TerminalFlowUpload obj=v1._1();
//				return new Tuple2<TerminalFlowUpload,Integer>(obj,obj.getSleep());
//			}
//		});
//		JavaPairRDD<TerminalFlowUpload, Integer> predict = train_data.mapToPair(new PairFunction<Tuple2<TerminalFlowUpload,Vector>, TerminalFlowUpload, Integer>() {
//			private static final long serialVersionUID = -1853987685961389911L;
//			@Override
//			public Tuple2<TerminalFlowUpload, Integer> call(Tuple2<TerminalFlowUpload, Vector> v1) throws Exception {
//				TerminalFlowUpload obj=v1._1();
//				Vector features = v1._2();
//				double score = model.predict(features);
//				int predict = score<1?0:(score<2?1:2);//为阈值
////				obj.setSleep(predict);
//				return new Tuple2<TerminalFlowUpload, Integer>(obj,predict);
//			}
//		});
//		JavaPairRDD<TerminalFlowUpload, Tuple2<Integer, Integer>> actual_predict = actual.join(predict);
//		double total_correct = actual_predict.mapToDouble(new DoubleFunction<Tuple2<TerminalFlowUpload, Tuple2<Integer, Integer>>>() {
//			private static final long serialVersionUID = -6084429523736739772L;
//			@Override
//			public double call(Tuple2<TerminalFlowUpload, Tuple2<Integer, Integer>> t) throws Exception {
//				Tuple2<Integer, Integer> point = t._2();
//				if (point._1() == point._2()){
//					return 1.0;
//				}
//				return 0.0;
//			}
//		}).sum();
//		JavaRDD<Tuple2<Object,Object>> predict_actual = actual_predict.map(new Function<Tuple2<TerminalFlowUpload, Tuple2<Integer, Integer>>,Tuple2<Object,Object>>(){
//			private static final long serialVersionUID = 5963181974159043336L;
//			@Override
//			public Tuple2<Object,Object> call(Tuple2<TerminalFlowUpload, Tuple2<Integer, Integer>> t) throws Exception {
//				Tuple2<Integer, Integer> point = t._2();
//				return new Tuple2<Object,Object>(point._2(),point._1());
//			}
//		});
		double total_correct = train_data.mapToDouble(new DoubleFunction<Tuple2<TerminalFlowUpload,Vector>>() {
			private static final long serialVersionUID = -4100165473415612945L;
			@Override
			public double call(Tuple2<TerminalFlowUpload, Vector> v1) throws Exception {
				TerminalFlowUpload obj=v1._1();
				Vector features = v1._2();
				double score = model.predict(features);
				int predict = score<1?0:(score<2?1:2);//为阈值
				if (predict == obj.getSleep()){
					return 1.0;
				}
				return 0.0;
			}
		}).sum();
		
		JavaRDD<Tuple2<Object,Object>> predict_actual = train_data.map(new Function<Tuple2<TerminalFlowUpload,Vector>, Tuple2<Object,Object>>() {
			private static final long serialVersionUID = -1889937958312764481L;
			@Override
			public Tuple2<Object, Object> call(Tuple2<TerminalFlowUpload, Vector> v1) throws Exception {
				TerminalFlowUpload obj=v1._1();
				Vector features = v1._2();
				double score = model.predict(features);
				double predict = score<1?0:(score<2?1:2);//为阈值
				double actual = obj.getSleep();
				return new Tuple2<Object,Object>(predict,actual);
			}
		});

		double accuracy = Double.valueOf(total_correct) / train.count();
		BinaryClassificationMetrics metrics = new BinaryClassificationMetrics(predict_actual.rdd());
		String print = "-->Accuracy:"+accuracy*100+"%,PR:"+metrics.areaUnderPR()*100+"%,ROC:"+metrics.areaUnderROC()*100+"%---";
//		System.out.println("---Accuracy:"+accuracy*100+"%,PR:"+metrics.areaUnderPR()*100+"%,ROC:"+metrics.areaUnderROC()*100+"%");
		
//		MongoSpark.save(train_data.map(new Function<Tuple2<TerminalFlowUpload,Vector>, Document>() {
//			private static final long serialVersionUID = -5282835007091769550L;
//			@Override
//			public Document call(Tuple2<TerminalFlowUpload, Vector> v1) throws Exception {
//				TerminalFlowUpload obj=v1._1();
//				double score = model.predict(v1._2());
//				int predict = score<1?0:(score<2?1:2);//为阈值
//				obj.setSleep(predict);
//				return Document.parse(JSON.toJSONString(obj));
//			}
//		}));
//		MongoSpark.save(test_data.map(new Function<Tuple2<TerminalFlowUpload,Vector>, Document>() {
//			private static final long serialVersionUID = -5282835007091769550L;
//			@Override
//			public Document call(Tuple2<TerminalFlowUpload, Vector> v1) throws Exception {
//				TerminalFlowUpload obj=v1._1();
//				double score = model.predict(v1._2());
//				int predict = score<1?0:(score<2?1:2);//为阈值
//				obj.setSleep(predict);
//				return Document.parse(JSON.toJSONString(obj));
//			}
//		}));
		
		return print;
	}
	
	
	public static void main(String[] args) {
		long start = System.currentTimeMillis();
		String result = "";
		long[] boundarys = new long[]{50*1000l,100*1000l,150*1000l,200*1000l,250*1000l,300*1000l,350*1000l,400*1000l,500*1000l};
		long s_boundary = 150*1000l;
		double[] relatives = new double[]{0.3,0.5,0.8,1};
		double s_relative = 0.5;
		int[] maxDepths = new int[]{5,10,15,20};
		int s_maxDepth = 10;
		int numClasses = 3;
		int[] maxBinss = new int[]{8,16,20,24,32};
		int s_maxBins = 32;
		VsimSleepML vdml = new VsimSleepML();
		for (long boundary : boundarys) {
			String code = vdml.ml(boundary, s_relative, s_maxDepth, numClasses, s_maxBins);
			String print = "[boundary:【"+boundary+"】,relative:"+s_relative+",maxDepth:"+s_maxDepth+",numClasses:"+numClasses+",maxBins:"+s_maxBins+"]"+code;
			System.out.println(print);
			result+="\n"+print;
		}
		for (double relative : relatives) {
			String code = vdml.ml(s_boundary, relative, s_maxDepth, numClasses, s_maxBins);
			String print = "[boundary:"+s_boundary+",relative:【"+relative+"】,maxDepth:"+s_maxDepth+",numClasses:"+numClasses+",maxBins:"+s_maxBins+"]"+code;
			System.out.println(print);
			result+="\n"+print;
		}
		for (int maxDepth : maxDepths) {
			String code = vdml.ml(s_boundary, s_relative, maxDepth, numClasses, s_maxBins);
			String print = "[boundary:"+s_boundary+",relative:"+s_relative+",maxDepth:【"+maxDepth+"】,numClasses:"+numClasses+",maxBins:"+s_maxBins+"]"+code;
			System.out.println(print);
			result+="\n"+print;
		}
		for (int maxBins : maxBinss) {
			String code = vdml.ml(s_boundary, s_relative, s_maxDepth, numClasses, maxBins);
			String print = "[boundary:"+s_boundary+",relative:"+s_relative+",maxDepth:"+s_maxDepth+",numClasses:"+numClasses+",maxBins:【"+maxBins+"】]"+code;
			System.out.println(print);
			result+="\n"+print;
		}
		String code = vdml.ml(150*1000l, 0.5, 15, 3, 16);
		System.out.println("==============================================================================");
		System.out.println(result);
		System.out.println("==============================================================================");
		String print = "[boundary:【150*1000】,relative:【0.5】,maxDepth:【15】,numClasses:3,maxBins:【16】]"+code;
		System.out.println(print);
		long end = System.currentTimeMillis();
		double time = (end - start) / 1000.00;
		double ss = time % 60;
		int mm = Double.valueOf(time / 60).intValue() % 60;
		int hh = Double.valueOf(time / 60 / 60).intValue() % 60;
		System.out.println("-------------------------classify耗时:"+(hh>0?hh+"小时":"")+(mm>0?mm+"分钟":"")+ss+"秒"+"-------------------------");
	}
}
