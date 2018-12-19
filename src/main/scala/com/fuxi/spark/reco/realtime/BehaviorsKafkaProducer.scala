package com.fuxi.spark.reco.realtime

import java.util.Properties

import com.fuxi.spark.util.KafkaRedisProperties
import kafka.javaapi.producer.Producer
import kafka.producer.{KeyedMessage, ProducerConfig}
import org.training.spark.proto.Spark.NewClickEvent

object BehaviorsKafkaProducer {
  val newClickEvents = Seq(
    (10L, 912L),
    (20L, 1468L),
    (100L, 2628L),
    (150L,1192L)
  )

  def run(topic:String) {
    val props: Properties = new Properties()
    props.put("metadata.broker.list", KafkaRedisProperties.KAFKA_ADDR)
    props.put("serializer.class", "kafka.serializer.DefaultEncoder")
    val conf: ProducerConfig = new ProducerConfig(props)

    var producer: Producer[String, Array[Byte]] = null

    try {
      System.out.println("Producing message")
      producer = new Producer[String, Array[Byte]](conf)
      for (event <- newClickEvents) {
        val eventProto = NewClickEvent.newBuilder().setUserId(event._1).setItemId(event._2).build()
        producer.send(new KeyedMessage[String, Array[Byte]](topic, eventProto.toByteArray))
        println("Sending message:" + eventProto.toString)

      }
      println("Done sending messages")
    } catch {
      case ex: Exception => {
        println("Error while producing messages:" + ex)
      }
    } finally {
      if (producer != null) producer.close
    }
  }
  @throws (classOf[Exception])
  def main(args:Array[String]): Unit = {
    BehaviorsKafkaProducer.run(KafkaRedisProperties.KAFKA_RECO_TOPIC)
  }

}
