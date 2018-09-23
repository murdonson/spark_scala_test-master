package practice

import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object KafkaWordCount {


  def main(args: Array[String]): Unit = {

//    val a=Array("192.168.32.138:7888,192.168.32.138:7889,192.168.32.138:7890","dj","wordcount",2)

    val zkQuorum="192.168.36.138:2181,192.168.36.138:2182,192.168.36.138:2183"
    val conf=new SparkConf()
      .setMaster("local[2]")
      .setAppName("dj")

    val ssc=new StreamingContext(conf,Seconds(4))

    val topicMap=Map("wordcount"->2)
    val lines=KafkaUtils.createStream(ssc,zkQuorum,"dj",topicMap).map(_._2)
    val words=lines.flatMap(lines=>lines.split(" "))
    val wordCount=words.map(x=>(x,1)).reduceByKey(_+_)


    wordCount.print()
    ssc.start()
    ssc.awaitTermination()





  }

}
