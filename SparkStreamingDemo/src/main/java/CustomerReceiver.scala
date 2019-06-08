import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver

class CustomerReceiver(host : String,port : Int) extends Receiver[String](storageLevel = StorageLevel.MEMORY_ONLY){

  //接收器启动的时候启动，接受器需要一个单独的线程
  override def onStart(): Unit = {
    val thread = new Thread("recevier"){
      override def run(): Unit = {
        recevier()
      }
    }

    thread.start()
  }

  def recevier(): Unit ={
    var socket: Socket = null
    var input: String = null
    try {
      socket = new Socket(host,port)

      //生成输入流
      val reader = new BufferedReader(new InputStreamReader(socket.getInputStream))

      //接受数据
      input = reader.readLine()
//
//      store()

      while (!isStopped() && input != null){
        store(input)
        input = reader.readLine()
      }

      restart("restart")

    }catch {
      case e: java.net.ConnectException => restart("restart")
      case t: Throwable => restart("restart")
    }
  }

  override def onStop(): Unit = {}
}
