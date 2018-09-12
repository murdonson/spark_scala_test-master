import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

object TransformBlacklist {

  def main(args: Array[String]): Unit = {
    //    val zkQuorum="192.168.36.138:2181,192.168.36.138:2182,192.168.36.138:2183"
    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("dj")

    val ssc = new StreamingContext(conf, Seconds(2))
    val blacklist = Array(("tom", true))
    val blacklistRdd = ssc.sparkContext.parallelize(blacklist, 2)


    val lineDstream = ssc.socketTextStream("hadoop000", 9999)
    val pairDstream = lineDstream.map(lineDstream => (lineDstream.split(" ")(1), lineDstream))

    val resultDstream = pairDstream.transform(pairRdd => {
      val joinedRdd = pairRdd.leftOuterJoin(blacklistRdd)
      println(joinedRdd.collect().toBuffer)
      val filterRdd = joinedRdd.filter(tuple => {
        println(tuple._2._2.getOrElse(false))
        if (tuple._2._2.getOrElse(false))
          false
        else {
          true
        }
      })

      filterRdd.map(x => x._2._1)
    })

    resultDstream.print()
    ssc.start()
    ssc.awaitTermination()


  }


}
