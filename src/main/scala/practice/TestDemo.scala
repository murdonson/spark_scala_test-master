package practice

import com.alibaba.fastjson.JSONObject
import org.apache.spark.sql.SparkSession

object TestDemo {

  def main(args: Array[String]): Unit = {

//    val event=new JSONObject()
//    event.put("id",3)
//    event.put("price",4)
//    println(event.toJSONString.getClass)

//    println("/class/130.html".split("/").toBuffer(0))
//    println("/class/130.html".split("/").toBuffer(1))

    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
        .master("local[2]")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._

    val df=spark.read.json("people.json")
//    df.show()

    df.filter($"age">21).show()

    val peopleDS = spark.read.json("people.json").as[Person]
  }

}
