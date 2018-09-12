package Order

import java.util.Properties

import com.alibaba.fastjson.JSONObject
import kafka.producer.{KeyedMessage, Producer, ProducerConfig}

import scala.util.Random


/**
  *   订单生产者
  */
object OrderProducer {

  def main(args: Array[String]): Unit = {
    val topic="order"
    val brokers="hadoop000:9092,hadoop000:9093,hadoop:9094"
    val props=new Properties()
    props.put("metadata.broker.list",brokers)
    props.put("serializer.class","kafka.serializer.StringEncoder")
    val kafkaConfig=new ProducerConfig(props)
    val producer=new Producer[String,String](kafkaConfig)


    //模拟订单
    while(true){
      val id=Random.nextInt(10)
      //订单事件
      val event=new JSONObject()
      event.put("id",id)
      val price=Random.nextInt(2000)
      event.put("price",price)

      println("id:"+id+"price:"+price)

      // 发送消息
      producer.send(new KeyedMessage[String,String](topic,event.toString))
      Thread.sleep(Random.nextInt(1000))

    }


  }



}
