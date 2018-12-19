package com.fuxi.spark.reco.realtime

import com.fuxi.spark.util.{KafkaRedisProperties, RedisClient}
import kafka.serializer.{DefaultDecoder, StringDecoder}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.training.spark.proto.Spark.NewClickEvent

object RealTimeRecommender {
  def main(args: Array[String]): Unit = {
    val Array(brokers,topics) = Array(KafkaRedisProperties.KAFKA_ADDR,KafkaRedisProperties.KAFKA_RECO_TOPIC)

    //create context with 2 second batch interval

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RealtimeRecommender")
    val ssc = new StreamingContext(sparkConf,Seconds(5))

    //create direct kafka stream with brokers and topics
    val topicSet = topics.split(",").toSet
    val kafkaParams = Map[String,String](
      "metadata.broker.list" -> brokers
//      "auto.offset.reset" -> "smallest"
    )

    val messages = KafkaUtils.createDirectStream[String,Array[Byte],StringDecoder,DefaultDecoder](ssc,kafkaParams,topicSet)

    //mapPartition是对rdd中的每个分区的迭代器进行操作
    //map是对rdd中的每一个元素进行操作，如果map过程中需要频繁创建额外的对象(例如将rdd中的数据通过jdbc写入数据库
    //map需要为每个元素创建一个连接而mapPartition为每个partition创建一个链接，则mapPartition效率比map高的多
//    messages.map(_._2).map{ event =>
//      NewClickEvent.parseFrom(event)
//    }.mapPartitions { iter =>
//      val jedis = RedisClient.pool.getResource
//      iter.map { event =>
//        println("NewClickEvent:" + event)
//        val userId = event.getUserId
//        val itemId = event.getItemId
//        val key = "II:" + itemId
//        val value = jedis.get(key)
//        if (value != null) {
//          jedis.set("RUI:" + userId, value)
//          println("Finish recommendation to user:" + userId)
//        }
//      }
//    }.print()
    messages.map(_._2).map { event =>
      NewClickEvent.parseFrom(event)
    }.foreachRDD({ rdd =>
      rdd.foreachPartition({ partitionOfRecords =>
        val jedis = RedisClient.pool.getResource
        partitionOfRecords.foreach({ event =>
          println("NewClickEvent:" + event)
          val userId = event.getUserId()
          val itemId = event.getItemId()
          val key = "II:" + itemId
          val value = jedis.get(key.getBytes())
          if(value != null) {
            val recoKey = "RUI:" + userId
            jedis.set(recoKey.getBytes(),value)
            println("Finish recommendation to user:" + userId)
          }
        })
      })
    })

    ssc.start()
    ssc.awaitTermination()


  }
}
