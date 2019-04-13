import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.rdd.RDD
import org.joda.time.{DateTime, Duration}


object RunDecisionTreeBinary {

  def initSpark(): SparkContext={
    val sparkConf = new SparkConf().setAppName("classification")
    val sc = new SparkContext(sparkConf)
    return sc
  }

  def pepareData(sc: SparkContext) : (RDD[LabeledPoint],RDD[LabeledPoint],RDD[LabeledPoint],Map[String,Int]) ={
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

  def evaluateModel(model: DecisionTreeModel, validData: RDD[LabeledPoint]): (Double) ={
    val scoreAndLabel = validData.map { x =>
      val predict = model.predict(x.features)
      (predict,x.label)
    }
    val metrics = new BinaryClassificationMetrics(scoreAndLabel)
    val auc = metrics.areaUnderROC()
    return auc

  }

  def trainEvaluate(trainData: RDD[LabeledPoint],validData: RDD[LabeledPoint]):DecisionTreeModel={
    println("开始训练")
    val startTime = new DateTime()
    val model = DecisionTree.trainClassifier(trainData,2,Map[Int,Int](),"entropy",10,10)
    val endTime = new DateTime()
    val auc = evaluateModel(model,validData)
    val duration = new Duration(startTime,endTime)
    println("训练花费了时间是："+duration.getMillis)
    
    return model
  }

  def predictData(sc: SparkContext, model: DecisionTreeModel, categoriesMap: Map[String, Int])={
    val rawDataWithHead = sc.textFile("hdfs://master:9000/user/hduser/test/test.tsv")
    val rawData = rawDataWithHead.mapPartitionsWithIndex{(index,iteration)=> if (index ==0 ) iteration.drop(1) else iteration}
    val lines = rawData.map(x => x.split("\t"))
    val dataRDD = lines.take(20).map{x =>
      val trimField = x.map(field => field.replaceAll("\"",""))
      val categoryFeaArray = Array.ofDim[Double](categoriesMap.size)
      val categoryIndex = categoriesMap(x(3))
      categoryFeaArray(categoryIndex) = 1
      val numField = trimField.slice(4,x.size).map{a => if (a == "?") 0.0 else a.toDouble}
      val url = x(0)
      val features = Vectors.dense(categoryFeaArray++numField)
      val predict = model.predict(features).toInt
      val predictDesc = {predict match {case 0 => "ephemeral"; case 1 => "evergreen";}}
      println(url+predictDesc)
    }
  }

  def evaluateParams(trainData: RDD[LabeledPoint], validData: RDD[LabeledPoint], impurityArr: Array[String], maxDepthArr: Array[Int], maxBinsArr: Array[Int]): DecisionTreeModel={
    val evaluationArrays = for (impurity <- impurityArr; maxdep <- maxDepthArr; maxBins <- maxBinsArr) yield {
      val model = DecisionTree.trainClassifier(trainData,2,Map[Int,Int](),impurity,maxdep,maxBins)
      val auc = evaluateModel(model,validData)
      (impurity,maxdep,maxBins,auc)
    }
    val bestEval = (evaluationArrays.sortBy(x => x(4)).reverse)(0)
    //Tuple 下标从1开始
    val bestModel = DecisionTree.trainClassifier(trainData.union(validData),2,Map[Int,Int](),bestEval._1,bestEval._2,bestEval._3)
  }

  def main(args: Array[String]): Unit = {
    val sc = initSpark()
    val (trainData, validData, testData, categoryMap) = pepareData(sc)
    val impurityArr = Array("gini","entropy")
    val maxdepthArr = Array(3,5,10,15,20)
    val maxBinArr = Array(3,5,10,50,100)
    val bestModel = evaluateParams(trainData,validData,impurityArr,maxdepthArr,maxBinArr)
    predictData(sc,bestModel,categoryMap)

  }

}
