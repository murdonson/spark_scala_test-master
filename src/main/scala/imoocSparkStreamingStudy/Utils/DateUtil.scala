package imoocSparkStreamingStudy.Utils

import java.util.Date

import org.apache.commons.lang3.time.FastDateFormat


object DateUtil {
  val originalFormat=FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")
  val targetFormat=FastDateFormat.getInstance("yyyyMMddHHmmss")

  def parseTime(time:String): String ={
    targetFormat.format(new Date(getTime(time)))

  }

  def getTime(time:String): Long ={
    originalFormat.parse(time).getTime
  }




}
