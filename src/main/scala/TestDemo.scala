import com.alibaba.fastjson.JSONObject

object TestDemo {

  def main(args: Array[String]): Unit = {

    val event=new JSONObject()
    event.put("id",3)
    event.put("price",4)
    println(event.toJSONString.getClass)

  }

}
