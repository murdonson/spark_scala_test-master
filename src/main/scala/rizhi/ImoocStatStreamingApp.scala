package rizhi

import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ListBuffer

object ImoocStatStreamingApp {

  def main(args: Array[String]): Unit = {

    if(args.length<4) {
        println("Usage: ImoocStatStreamingApp <zkQuorum> <group> <topics> <numThreads>")
        System.exit(1)
    }

    val Array(zkQuorum,group,topic,numThreads)=args
    val sparkconf=new SparkConf().setAppName("dj")  //.setMaster("local[2]")
    val ssc=new StreamingContext(sparkconf,Seconds(60))

    val topicMap=topic.split(",").map((_,numThreads.toInt)).toMap
    val messages=KafkaUtils.createStream(ssc,zkQuorum,group,topicMap)

//    messages.map(_._2).count().print()

    val logs=messages.map(_._2)

    val filterlogs=logs.filter(line=>{
      line.split("\t").length>=5
    })

    val cleanData=filterlogs.map(line=>{
      val infos=line.split("\t")

        val url=infos(2).trim().split(" ")(1)
        var courseId=0
        // url=/class/112.html
        if(url.startsWith("/class")){
          courseId=url.split("/")(2).substring(0,url.split("/")(2).lastIndexOf(".")).toInt
        }


//      println("***************infos***************\n"+infos.toBuffer)
      ClickLog(infos(0), DateUtils.parseToMinute(infos(1)), courseId, infos(3).toInt, infos(4))
    }).filter(line=>line.courseId!=0)



      // 统计访问量
    cleanData.map(x => {

      // HBase rowkey设计： 20171111_88

      (x.time.substring(0, 8) + "_" + x.courseId, 1)
    }).reduceByKey(_ + _).foreachRDD(rdd => {
      rdd.foreachPartition(partitionRecords => {
        val list = new ListBuffer[CourseClickCount]

        partitionRecords.foreach(pair => {
          list.append(CourseClickCount(pair._1, pair._2))
        })

        CourseClickCountDAO.save(list)
      })
    })



    // 统计通过搜索引擎转换过来的次数
    cleanData.map(x => {

      /**
        * https://www.sogou.com/web?query=Spark SQL实战
        *
        * ==>
        *
        * https:/www.sogou.com/web?query=Spark SQL实战
        */
      val referer = x.referer.replaceAll("//", "/")
      val splits = referer.split("/")
      var host = ""
      if(splits.length > 2) {
        host = splits(1)
      }

      (host, x.courseId, x.time)
    }).filter(_._1 != "").map(x => {
      (x._3.substring(0,8) + "_" + x._1 + "_" + x._2 , 1)
    }).reduceByKey(_ + _).foreachRDD(rdd => {
      rdd.foreachPartition(partitionRecords => {
        val list = new ListBuffer[CourseSearchClickCount]

        partitionRecords.foreach(pair => {
          list.append(CourseSearchClickCount(pair._1, pair._2))
        })

        CourseSearchClickCountDAO.save(list)
      })
    })
    ssc.start()
    ssc.awaitTermination()

  }


}
