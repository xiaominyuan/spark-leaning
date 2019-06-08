import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object WordCount {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("wordCount").setMaster("local[*")
    val ssc = new StreamingContext(conf, Seconds(6))

//    val lineDStream = ssc.socketTextStream("host",9999)

    val lineDStream = ssc.receiverStream(new CustomerReceiver("localhost",9999))
    val wordsDStream = lineDStream.flatMap(x => x.split(" "))
    val kvDStream = wordsDStream.map(x => (x,1))
    val result = kvDStream.reduceByKey((x,y) => (x+y))

    result.print()
    ssc.start()

  }
}
