package movieRecommender.Order

import com.alibaba.fastjson.JSON
import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object OrderConsumer {

  val dbIndex=0

  // 每件商品总销售额
  val totalPerProductKey="order::totalPerProduct"

  val  oneMinTotalPerProcutKey="order::oneMinTotalPerProcut"

  val totalKey="order::all"

  def main(args: Array[String]): Unit = {

    // 创建 StreamingContext 时间片为1秒
    val conf = new SparkConf().setMaster("local[2]").setAppName("UserClickCountStat")
    val ssc = new StreamingContext(conf, Seconds(1))

    val topics=Set("order")
    val brokers="hadoop000:9092,hadoop000:9093,hadoop:9094"
    val kafkaParams=Map[String,String](
      "metadata.broker.list"->brokers,
      "serializer.class"->"kafka.serializer.StringEncoder"
    )

    // 创建一个 direct stream
    val kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)

    val events=kafkaStream.flatMap(x=>Some(JSON.parseObject(x._2)))
    //val orders = events.map(x => (x.getString("id"), x.getLong("price"))).groupByKey().map(x => (x._1, x._2.size, x._2.reduceLeft(_ + _)))

    //  商品id 商品销量 商品总价
    val orders=events
      .map(x=>(x.getString("id"),x.getString("price")))
      .groupByKey()
      .map(x=>(x._1,x._2.size,x._2.reduceLeft(_+_)))

    orders.foreachRDD(x=>x.foreachPartition(x=>{
      //每个partition的每个rdd
      x.foreach(x=>{

        val jedis=RedisClient.pool.getResource
        jedis.select(dbIndex)
        // 商品id 这件商品总销售额  每个商品销售额累加
        jedis.hincrBy(totalPerProductKey, x._1, x._3.toLong)

        // 上一分钟 每个商品销售额
        jedis.hset(oneMinTotalPerProcutKey,x._1.toString,x._3.toString)
        // 总销售额累加
        jedis.incrBy(totalKey,x._3.toLong)
        RedisClient.pool.returnResource(jedis)
      })
    }))



    ssc.start()
    ssc.awaitTermination()


  }

}
