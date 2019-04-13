
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

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

  }
}
