package imoocSparkStreamingStudy

import imoocSparkStreamingStudy.Utils.DateUtil
import kafka.serializer.StringDecoder
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import rizhi._

import scala.collection.mutable.ListBuffer

object ImoocStatStreamingApp {

  def main(args: Array[String]): Unit = {

    if(args.length<4){
      println("usage: <zkQuorum> <group> <topic> <numThreads> ")
      System.exit(1)
    }

    val Array(zkQuorum,group,topic,numThreads)=args
    val sparkConf=new SparkConf()
      .setAppName("sparkStreamingStudy")
      .setMaster("local[2]")

    val ssc=new StreamingContext(sparkConf,Seconds(60))

    val topicSet=topic.split(",").toSet
    val kafkaParams=Map[String,String]("metadata.broker.list"->zkQuorum)

    val messages=KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc,kafkaParams ,topicSet)

    val lines=messages.map(_._2)


    // 清洗数据 组装成clickLog的形式
    //10.123.234.126  2018-09-20 09:37:01      GET /class/130.html http/1.1   404     http://cn.bing.com/search?q=hadoop基础
    val filterLines=lines.filter(line=>{
      line.split("\t").length>=5
    })

    // case class ClickLog(ip:String, time:String, courseId:Int, statusCode:Int, referer:String)
    val cleanData=filterLines.map(line=>{
      val infos=line.split("\t")
      // url /class/130.html
      val url=infos(2).trim().split(" ")(1)
      val courseIdHtml=url.split("/")(2)
      val courseId=courseIdHtml.substring(0,courseIdHtml.lastIndexOf(".")).toInt

      ClickLog(infos(0),DateUtil.parseTime(infos(1)),courseId,infos(3).toInt,infos(4))
    })


    // HBase rowkey设计： 20171111_88
    //  统计从今天开始到现在为止 每个课程的访问量
    cleanData.map(line=>{
      (line.time.substring(0,8)+"_"+line.courseId,1)
    }).reduceByKey(_+_).foreachRDD(rdd=>{
      rdd.foreachPartition(partitionRecords=>{
        val list=new ListBuffer[CourseClickCount]

        partitionRecords.foreach(pair=>{
          list.append(CourseClickCount(pair._1,pair._2))
        })

        CourseClickCountDAO.save(list)
      })
    })


    cleanData.map(x=>{
      //             http://cn.bing.com/search?q=hadoop基础
      val referer=x.referer.replaceAll("//","/")
      val splits=referer.split("/")
      var hosts=""
      if(splits.length>2){
        hosts=splits(1)
      }
      (hosts,x.time,x.courseId)
    }).filter(_._1!="").map(x=>{
      (x._2.substring(0,8)+"_"+x._1+"_"+x._2,1)
    }).reduceByKey(_+_).foreachRDD(rdd=>{
      rdd.foreachPartition(partitionRecords=>{

        val list=new ListBuffer[CourseSearchClickCount]

        partitionRecords.foreach(pair=>{
          list.append(CourseSearchClickCount(pair._1,pair._2))
        })

        CourseSearchClickCountDAO.save(list)

      })
    })

    ssc.start()
    ssc.awaitTermination()
  }

}
