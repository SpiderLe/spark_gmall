package com.king.yl.realtime.app

import com.alibaba.fastjson.JSON
import com.king.yl.common.util.MyEsUtil
import com.king.yl.constant.GmallConstant
import com.king.yl.realtime.bean.OrderInfo
import com.king.yl.util.MyKafkaUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object OrderApp {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setAppName("oder_app").setMaster("local[*]")

    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val inputDstream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstant.KAFKA_TOPIC_ORDER, ssc)

    // 转换格式，补充时间, 部分数据脱敏
    val orderInfoDstream: DStream[OrderInfo] = inputDstream.map { record =>

      val jsonStr: String = record.value()
      val orderInfo: OrderInfo = JSON.parseObject(jsonStr, classOf[OrderInfo])

      val orderTimeArr: Array[String] = orderInfo.createTime.split(" ")
      orderInfo.createDate = orderTimeArr(0)
      orderInfo.createHour = orderTimeArr(1).split(":")(0)
      orderInfo.createHourMinute = orderTimeArr(1)

      //数据脱敏
      val tel1: (String, String) = orderInfo.consigneeTel.splitAt(4)
      val tel2: (String, String) = orderInfo.consigneeTel.splitAt(7)
      //  13810100101=> 1381***0101
      orderInfo.consigneeTel = tel1._1 + "****" + tel2._2

      orderInfo
    }

    //orderinfo增加一个字段 来区别是否是用户首次下单

    //保存到ES
    orderInfoDstream.foreachRDD{rdd =>
      rdd.foreachPartition{ orderInfoItr =>
        MyEsUtil.insertBulk(GmallConstant.ES_INDEX_ORDER, orderInfoItr.toList)
      }
    }




    ssc.start()
    ssc.awaitTermination()



  }

}
