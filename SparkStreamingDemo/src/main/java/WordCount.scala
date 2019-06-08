import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object WordCount {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("wordCount").setMaster("local[*")
    val ssc = new StreamingContext(conf, Seconds(6))

    ssc.checkpoint("./checkPoint")

//    val lineDStream = ssc.socketTextStream("host",9999)

    val lineDStream = ssc.receiverStream(new CustomerReceiver("localhost",9999))
    val wordsDStream = lineDStream.flatMap(x => x.split(" "))
    val kvDStream = wordsDStream.map(x => (x,1))


    val updateFunction = (v : Seq[Int], state : Option[Int]) => {

      //getOrElse：如果option这个容器中没有值，返回0
      val preState = state.getOrElse(0)
      Some(preState + v.sum)
    }
    val result = kvDStream.updateStateByKey(updateFunction)

    val results = kvDStream.reduceByKeyAndWindow((x:Int, y:Int) => x+y, (a:Int,b :Int) => (a-b),Seconds(18),Seconds(12))
    
//    val result = kvDStream.reduceByKey((x,y) => (x+y))

    result.print()
    ssc.start()

  }
}
