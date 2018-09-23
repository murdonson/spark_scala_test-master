package movieRecommender.demo

import org.apache.spark.{SparkConf, SparkContext}

object splitDataSet {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("splitDataSet").setMaster("local[2]")
    //创建sparkContext,该对象是提交spark App的入门
    val sc = new SparkContext(conf)

    val ratings=sc.textFile("data/ratings.txt").map{
      line=>{
        try {
          val fields = line.split("\\s+")
          (fields(0).toInt, fields(1).toInt, fields(2).toDouble, fields(3).toLong % 10)
        } catch {
          case e: Exception =>(0,0,0.toDouble,0.toLong)
        }
      }
    }

    val training=ratings.filter(x=>x._4<6).saveAsTextFile("data/train")
    val validation=ratings.filter(x=>x._4>=6&&x._4<8).saveAsTextFile("data/validation")
    val text=ratings.filter(x=>x._4>8).saveAsTextFile("data/test")

   // val training=ratings.filter(x=>x._4<6&&x._1!=0 ).saveAsTextFile(args(1))




  }


}
