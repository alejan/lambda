package main.scala.streaming

import kafka.serializer.StringDecoder
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}
import org.apache.spark.{SparkConf, SparkContext}


/**
  * Created by alejandroquintero on 20/03/17.
  */
object StreamingJob {

  def main(args: Array[String]): Unit = {

      val sparkConf = new SparkConf().setMaster("local[4]").setAppName("lambda streaming")

    val sc = new SparkContext(sparkConf)
    val batchDuration= Seconds(10)
    @transient  val ssc = new StreamingContext(sc, batchDuration)
    ssc.checkpoint("file:///usr/local/Cellar/kafka/checkpoint")

    val kafkaParams= Map(
      "metadata.broker.list"->"localhost:9092",
      "group.id"->"lambda",
      "auto.offset.reset"->"smallest"
    )



    val kafkaStream1=KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](
      ssc,kafkaParams,Set("postcliente")).map(_._2)
    val kafkaStream2=KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](
      ssc,kafkaParams,Set("rutas")).map(_._2)


  val tiendaOrigenDS = kafkaStream1.transform{line=>
    line.map(_.split(","))
      .map(attr=>(attr(0),attr(1).toDouble,attr(2).toDouble,attr(3),attr(4).toLong))
      .filter(_._5 > 70L)
    }


  val tiendaDestinoDS = kafkaStream1.transform{line=>
    line.map(_.split(","))
      .map(attr=>(attr(0),attr(1).toDouble,attr(2).toDouble,attr(3),attr(4).toLong))
      .filter(_._5 < 40L)
  }

  val rutaDS= kafkaStream2.transform{line=>
    line.map(_.split(","))
      .map(attr=>(attr(0),attr(1).toDouble,attr(2).toDouble,attr(3)))

   }

    tiendaOrigenDS.filter(_._4=="huevos").print()
    tiendaDestinoDS.filter(_._4=="huevos").print()
    rutaDS.filter(_._4=="disponible").print()

 var f=  rutaDS.slice(new Time(4000),new Time(8000))

    ssc.start()

   ssc.awaitTermination()
  }




}
