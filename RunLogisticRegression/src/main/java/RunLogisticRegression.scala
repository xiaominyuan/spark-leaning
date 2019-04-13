
import org.apache.spark.mllib.feature.StandardScaler
import org.apache.spark.mllib.linalg.Vectors
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
  }
}
