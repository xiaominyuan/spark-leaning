import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD

object Recommend {

  def main(args: Array[String]): Unit = {

    def SetLogger = {
      Logger.getLogger("org").setLevel(Level.OFF)
      Logger.getLogger("com").setLevel(Level.OFF)
      System.setProperty("spark.ui.showConsoleProgress","false")
      Logger.getRootLogger().setLevel(Level.OFF)
    }

    def PrepareData() : (RDD[Rating],Map[Int,String]) = {
      val sc = new SparkContext(new SparkConf().setAppName("recommend"))
      val rawUserData = sc.textFile("hdfs://master:9000/user/hduser/test/u.data")
      val rawRatings = rawUserData.map(x => x.split("\t").take(3))
      val ratingRDD = rawRatings.map{case Array(user,movie,rating) =>
      Rating(user.toInt,movie.toInt,rating.toDouble)}
      println("ratingRDD共计:"+ratingRDD.count().toString)

      println("开始读取电影数据")
      val itemRDD = sc.textFile("hdfs://master:9000/user/hduser/test/u.item")
      val movieTitle = itemRDD.map(x => x.split("\\|").take(2)).map(x => (x(0).toInt,x(1))).collectAsMap().toMap

      val numRatings = ratingRDD.count()
      val numUsers = ratingRDD.map(x => x.user).distinct().count()
      val numMovies = ratingRDD.map(x => x.product).distinct().count()

      println("numratings is "+numRatings+ " numUsers is "+numUsers+"  numMovies is "+numMovies)
      return (ratingRDD,movieTitle)
    }

    def recommendMovies(model : MatrixFactorizationModel, movieTitle : Map[Int,String]) = {
      val movies = model.recommendProducts(196,10)
      println("用户id 196"+"推荐下面的电影")
      movies.foreach{x =>
        println(movieTitle(x.product)+"分数是"+x.rating.toString)
      }
    }

    def recommendUsers(model : MatrixFactorizationModel, movieTitle: Map[Int,String]) ={
      val users = model.recommendUsers(100,10)
      println("电影"+movieTitle(100)+"推荐用户")
      users.foreach(x =>
      println(movieTitle(100)+"推荐用户是"+x.user.toString))
    }

    def recommend(model : MatrixFactorizationModel, movieTitle : Map[Int,String]) = {
      if (args(0) == 1){
        recommendMovies(model,movieTitle)
      }
      if (args(1) ==2){
        recommendUsers(model,movieTitle)
      }

    }


    val ratingRDD_movieTitle = PrepareData()
    val (ratingRDD,movieTitle) = ratingRDD_movieTitle

    val model = ALS.train(ratingRDD,5,10,0.1)
    recommend(model,movieTitle)



  }
}
