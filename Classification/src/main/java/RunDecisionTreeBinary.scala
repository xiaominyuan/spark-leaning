import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.rdd.RDD
import org.joda.time.{DateTime, Duration}


object RunDecisionTreeBinary {

  def pepareData() : (RDD[LabeledPoint],RDD[LabeledPoint],RDD[LabeledPoint],Map[String,Int]) ={
    val sparkConf = new SparkConf().setAppName("classification")
    val sc = new SparkContext(sparkConf)
    val rawDataWithHeader = sc.textFile("hdfs://master:9000/user/hduser/test/train.tsv")
    val rawData = rawDataWithHeader.mapPartitionsWithIndex{(index,iter) => if (index == 0) iter.drop(1) else iter}
    val lines = rawData.map(x => x.split("\t"))
    println("一共多少条数据: "+lines.count())

    val categoriesMap = lines.map(x => x(3)).distinct.collect().zipWithIndex.toMap
    val labelpointRDD = lines.map{x =>
      val trFields = x.map(a => a.replaceAll("\"",""))
      val cateFeaArr = Array.ofDim[Double](categoriesMap.size)
      val cateIndex = categoriesMap(x(3))
      cateFeaArr(cateIndex)=1
      val numFea = trFields.slice(4,x.size-1).map(a => if(a=="?") 0.0 else a.toDouble)
      val label = trFields(x.size-1).toInt
      LabeledPoint(label,Vectors.dense(cateFeaArr++numFea))
    }
    val Array(trainData,validData,testData) = labelpointRDD.randomSplit(Array(0.8,0.1,0.1))

    return (trainData,validData,testData,categoriesMap)
  }

  def trainEvaluate(trainData: RDD[LabeledPoint],validData: RDD[LabeledPoint]):DecisionTreeModel={
    println("开始训练")
    val startTime = new DateTime()
    val model = DecisionTree.trainClassifier(trainData,2,Map[Int,Int](),"entropy",10,10)
    val endTime = new DateTime()
    val auc = 
    val duration = new Duration(startTime,endTime)
    println("训练花费了时间是："+duration.getMillis)
    return model
  }


}
