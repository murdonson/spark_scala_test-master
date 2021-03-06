package movieRecommender.Order

import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.JedisPool

object RedisClient extends Serializable{

  val redisHost="hadoop000"
  val redisPort=6379
  val redisTimeout=3000

  lazy val pool = new JedisPool(new GenericObjectPoolConfig(), redisHost, redisPort, redisTimeout)

  lazy val hook = new Thread {
    override def run = {
      println("Execute hook thread: " + this)
      pool.destroy()
    }
  }
  sys.addShutdownHook(hook.run)


  def main(args: Array[String]): Unit = {
    val dbIndex=0
    val jedis=RedisClient.pool.getResource
    jedis.select(dbIndex)
    jedis.set("test","1")
    println(jedis.get("test"))

    RedisClient.pool.returnResource(jedis)


  }



}
