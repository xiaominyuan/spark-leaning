

import org.apache.spark.mllib.classification.{LogisticRegressionModel, LogisticRegressionWithSGD}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.feature.StandardScaler
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.{DateTime, Duration}

object RunLogisticRegression {

  def initSpark():SparkContext={
    val sparkConf = new SparkConf().setAppName("logisticRegression")
    val sparkContext = new SparkContext(sparkConf)
    return sparkContext
  }

  def papareData(sparkContext: SparkContext): (RDD[LabeledPoint],RDD[LabeledPoint],RDD[LabeledPoint],Map[String,Int])={
    val rawWithHead = sparkContext.textFile("hdfs://master:9000/user/hduser/test/train.tsv")
    val rawData = rawWithHead.mapPartitionsWithIndex{(index,iteration) => if (index == 0) iteration.drop(1) else iteration}
    val lines = rawData.map(x => x.split("\t"))
    val categoryMap = lines.map(fields => fields(3)).distinct().collect().zipWithIndex.toMap

    val labeledPointRDD = lines.map{fields =>
      val trimFields = fields.map(x => x.replaceAll("\"",""))
      val categoryFeaArr = Array.ofDim[Double](categoryMap.size)
      val categoryIndex = categoryMap(fields(3))
      categoryFeaArr(categoryIndex) = 1
      val numericalFea = trimFields.slice(4,fields.size-1).map{x => if (x == "?") 0.0 else x.toDouble}
      val label = trimFields(fields.size-1).toInt
      val features = Vectors.dense(categoryFeaArr++numericalFea)
      LabeledPoint(label,features)
    }

    val featuresData = labeledPointRDD.map(x => x.features)
    val stdScaler = new StandardScaler(withMean = true, withStd = true).fit(featuresData)
    val scalerdRDD = labeledPointRDD.map(x => LabeledPoint(x.label,stdScaler.transform(x.features)))

    val Array(trainData, validData, testData) = scalerdRDD.randomSplit(Array(0.8,0.1,0.1))
    return (trainData,validData,testData,categoryMap)
  }

  def trainModel(trainData: RDD[LabeledPoint], numIterations: Int, stepSize: Int, miniBatchFraction: Double): (LogisticRegressionModel,Double)={
    val startTime = new DateTime()
    val model = LogisticRegressionWithSGD.train(trainData,numIterations,stepSize,miniBatchFraction)
    val endTime = new DateTime()
    val duration = new Duration(startTime,endTime)
    return (model,duration.getMillis)
  }

  def evaluationModel(model: LogisticRegressionModel, validData: RDD[LabeledPoint]): Double={
    val scoreAndLable = validData.map{x =>
      val predict = model.predict(x.features)
      (predict,x.label)
    }
    val metrics = new BinaryClassificationMetrics(scoreAndLable)
    val roc = metrics.areaUnderROC()
    return roc
  }

  def evaluationParam(trainData: RDD[LabeledPoint], validData:RDD[LabeledPoint], numIterArr: Array[Int], stepSizeArr: Array[Int], miniFraArr: Array[Double]): LogisticRegressionModel={
    val evaluationArr = for (numIter <- numIterArr; stepSize <- stepSizeArr; miniFra <- miniFraArr) yield {
      val model = trainModel(trainData,numIter,stepSize,miniFra)
      val roc = evaluationModel(model, validData)
      (numIter,stepSize,miniFra,roc)
    }
    val bestParam = (evaluationArr.sortBy(x => x(4)).reverse)(0)
    val bestModel = LogisticRegressionWithSGD.train(trainData.union(validData),bestParam._1,bestParam._2,bestParam._3)
    return bestModel
  }

  def predictTestData(sc: SparkContext, model: LogisticRegressionModel, categoryMap: Map[String,Int])={
    val rawWithHead = sc.textFile("hdfs://master:9000/user/hduser/test/test.tsv")
    val rawData = rawWithHead.mapPartitionsWithIndex{(index,iteration) => if (index == 0) iteration.drop(1) else iteration}
    val lines = rawData.map(x => x.split("\t"))
    val showTestData = lines.map{fields =>
      val trimData = fields.map(x => x.replaceAll("\"",""))
      val categoryFeatureArr = Array.ofDim[Double](categoryMap.size)
      val categoryFeatureIndex = categoryMap(fields(3))
      categoryFeatureArr(categoryFeatureIndex) = 1
      val numericFeatures = fields.slice(4,fields.size).map{x => if (x == "?") 0.0 else x.toDouble}
      val features = Vectors.dense(categoryFeatureArr++numericFeatures)
      val predict = model.predict(features).toInt
      val predictTrans = {predict match {case 0 => "ephemeral"; case 1 => "evergreen"}}
      val url = trimData(0)

      println(url+predictTrans)
    }
  }

  def main(args: Array[String]): Unit = {
    val sparkContext = initSpark()
    val (trainData, validData, testData, categoryMap) = papareData(sparkContext)
    trainData.persist()
    validData.persist()
    testData.persist()
    val numIterationArr = Array(5,10,20,60)
    val stepSizeArr = Array(10,50,100)
    val miniBatchArr = Array(0.5,0.8,1)

    val bestModel = evaluationParam(trainData,validData,numIterationArr,stepSizeArr,miniBatchArr)
    predictTestData(sparkContext,bestModel,categoryMap)
  }

}
