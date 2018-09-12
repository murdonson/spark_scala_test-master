import org.apache.spark.{SparkConf, SparkContext}

object GroupTopN {

  def main(args: Array[String]): Unit = {

    val conf=new SparkConf()
      .setMaster("local")
      .setAppName("dj")

    val sc=new SparkContext(conf)

    val lines=sc.textFile("data/sort.txt")

    val pairs=lines.map(line=>(line.split(" ")(0).toInt,line.split(" ")(1).toInt))

//    (4,CompactBuffer(5, 6, 7))
//    (3,CompactBuffer(4, 2, 7))
//    (5,CompactBuffer(6, 9, 34))
    val sort=pairs.groupByKey().map(line=>(line._1,line._2.toSeq.sortWith(_>_).take(2)))

    val result=sort.map(
      line=> {
        val list=line._2
        for(number<-list)
          yield (line._1,number)
      }
    )

    println(result.collect().toBuffer)

  }

}
