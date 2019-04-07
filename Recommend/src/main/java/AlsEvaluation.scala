import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.joda.time.{DateTime, Duration}

object AlsEvaluation {

  def pepareData2(): (RDD[Rating],RDD[Rating],RDD[Rating],Map[Int,String]) = {
    val sc = new SparkContext(new SparkConf().setAppName("evaluation"))
    val rawUserData = sc.textFile("hdfs://master:9000/user/hduser/test/u.data")
    val rawRating = rawUserData.map(x => x.split("\t").take(3))
    val ratingRDD = rawRating.map{case Array(user,movie,rating) => Rating(user.toInt,movie.toInt,rating.toDouble)}

    val itemRDD = sc.textFile("hdfs://master:9000/user/hduser/test/u.item")
    val movieTitle = itemRDD.map(x => x.split("\\|").take(2)).map(x => (x(0).toInt,x(1))).collectAsMap().toMap

    val Array(trainData,validData,testData) = ratingRDD.randomSplit(Array(0.8,0.1,0.1))
    return (trainData,validData,testData,movieTitle)
  }


  def computeRMSE(model: MatrixFactorizationModel, ratingRDD: RDD[Rating]): Double={
    val predictRDD = model.predict(ratingRDD.map(x => (x.user,x.product)))
    val predictAndActual = predictRDD.map(x => ((x.user,x.product),x.rating)).join(ratingRDD.map(x => ((x.user,x.product),x.rating))).values
    val sumNum = ratingRDD.count()
    return math.sqrt(predictAndActual.map(x => (x(2)-x(1))*(x(2)-x(1))).reduce(_+_) / sumNum)
  }

  def trainModel(trainData: RDD[Rating], validData: RDD[Rating], rank:Int, numItera:Int, lam:Double): (Double,Double)={
    val startTiime = new DateTime()
    val model = ALS.train(trainData,rank,numItera,lam)
    val endTime = new DateTime()
    val rmse = computeRMSE(model,validData)
    val times = new Duration(startTiime,endTime)
    println("训练花费时间"+times)
    println("rank is "+rank+" numItera is "+numItera+" lambda is "+lam+" and rmse is "+rmse.toString)
    return (rmse,times.toString.toDouble)

  }



  def evaluatePara(trainData:RDD[Rating], validData:RDD[Rating], rankArr:Array[Int],numIteraArr:Array[Int],lambdaArr:Array[Double]): MatrixFactorizationModel={
    val evaluations = for (rank <- rankArr; numItera <- numIteraArr; lam <- lambdaArr) yield {
      val (rmse,times) = trainModel(trainData,validData,rank,numItera,lam)
      (rank,numItera,lam,rmse)
    }
    val eval = (evaluations.sortBy(_._4))
    val bestEval = eval(0)
    println("最佳model参数: rank is"+bestEval._1+",iterations"+bestEval._2+",lambda is"+bestEval._3)
    val bestModel = ALS.train(trainData,bestEval._1,bestEval._2,bestEval._3)
    return bestModel
  }

  def trainValid(trainData:RDD[Rating],validData:RDD[Rating]):MatrixFactorizationModel = {
    println("网格搜索最优参数")
    val bestModel = evaluatePara(trainData,validData,Array(7,9,11),Array(10,15,20),Array(0.01,0.05,0.1))
    return bestModel
  }

  def main(args: Array[String]): Unit = {
    Recommend.SetLogger
    val (trainData,validData,testData,movieTitle) = pepareData2()
    val bestModel = trainValid(trainData,validData)
    val rmseResult = computeRMSE(bestModel,testData)
    println("最终的test的rmse的结果是"+rmseResult)

  }
}
