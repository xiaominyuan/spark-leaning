import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.joda.time.{DateTime, Duration}

object AlsEvaluation {

  def pepareData2(): (RDD[Rating],RDD[Rating],RDD[Rating]) = {
    val sc = new SparkContext(new SparkConf().setAppName("evaluation"))
    val rawUserData = sc.textFile("hdfs://master:9000/user/hduser/test/u.data")
    val rawRating = rawUserData.map(x => x.split("\t").take(3))
    val ratingRDD = rawRating.map{case Array(user,movie,rating) => Rating(user.toInt,movie.toInt,rating.toDouble)}

    val itemRDD = sc.textFile("hdfs://master:9000/user/hduser/test/u.item")
    val movieTitle = itemRDD.map(x => x.split("\\|").take(2)).map(x => (x(0).toInt,x(1))).collectAsMap().toMap

    val Array(trainData,validData,testData) = ratingRDD.randomSplit(Array(0.8,0.1,0.1))
    return (trainData,validData,testData)
  }


  def computeRMSE(model: MatrixFactorizationModel, ratingRDD: RDD[Rating]): Double={
    val predictRDD = model.predict(ratingRDD.map(x => (x.user,x.product)))
    val predictAndActual = predictRDD.map(x => ((x.user,x.product),x.rating)).join(ratingRDD.map(x => ((x.user,x.product),x.rating))).values
    val sumNum = ratingRDD.count()
    return math.sqrt(predictAndActual.map(x => (x(2)-x(1))*(x(2)-x(1))).reduce(_+_)/sumNum)
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



  def evaluatePara(trainData:RDD[Rating], validData:RDD[Rating], evaParam:String,
                   rankArr:Array[Int],numIteraArr:Array[Int],lambdaArr:Array[Double])={
    for (rank <- rankArr; numItera <- numIteraArr; lam <- lambdaArr){
      val (rmse, time) =trainModel(trainData,validData,rank,numItera,lam)
      
    }
  }

  def trainValid(trainData:RDD[Rating],validData:RDD[Rating]):MatrixFactorizationModel = {

  }

  def main(args: Array[String]): Unit = {
    Recommend.SetLogger


  }
}
