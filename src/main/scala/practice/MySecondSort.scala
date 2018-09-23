package practice

import org.apache.spark.{SparkConf, SparkContext}

object MySecondSort {

  def main(args: Array[String]): Unit = {

    val conf=new SparkConf()
      .setMaster("local")
      .setAppName("dj")

    val sc=new SparkContext(conf)

    val lines=sc.textFile("data/sort.txt")

    val pairs=lines.map(line=>(new MySecondSortKey(line.split(" ")(0).toInt,line.split(" ")(1).toInt),line))

    val res=pairs.sortByKey().map(line=>line._2)

    println(res.collect().toBuffer)



  }

}
