package main.scala.streaming


import com.mongodb.spark.{rdd, _}
import com.mongodb.spark.config.{MongoInputConfig, ReadConfig, WriteConfig}
import kafka.serializer.StringDecoder
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}
import org.apache.spark.{SparkConf, SparkContext}
import org.bson.Document




/**
  * Created by alejandroquintero on 20/03/17.
  */
object StreamingJob {

  val sparkConf = new SparkConf()
    .setMaster("local[4]")
    .setAppName("lambda streaming")
    .set("spark.mongodb.output.uri","mongodb://127.0.0.1:27017/almacendb.almacenesorig")
  val sc = SparkContext.getOrCreate(sparkConf)
sc.setCheckpointDir("/tmp/checkpoint")
  val batchDuration= Seconds(4)

  @transient  val ssc = new StreamingContext(sc, batchDuration)

  ssc.checkpoint("file:///usr/local/Cellar/kafka/checkpoint")

  val kafkaParams= Map(
    "metadata.broker.list"->"localhost:9092",
    "group.id"->"lambda",
    "auto.offset.reset"->"smallest"
  )


  var almacenesOrigenRDD: RDD[(String, Double,Double,String, Long)] = sc.emptyRDD
  var almacenesDestinoRDD: RDD[(String, Double,Double,String, Long)] = sc.emptyRDD

  val kafkaStream1=KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](
    ssc,kafkaParams,Set("postcliente")).map(_._2)

  val kafkaStream2=KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](
    ssc,kafkaParams,Set("rutas")).map(_._2)

  val writeConfig = WriteConfig(Map("collection" -> "almacenesorig", "writeConcern.w" -> "majority"), Some(WriteConfig(sc)))

  def main(args: Array[String]): Unit = {



    kafkaStream1.foreachRDD{line=>

            almacenesOrigenRDD= line.map(_.split(","))
               .map(attr=>(attr(0),attr(1).toDouble,attr(2).toDouble,attr(3),attr(4).toLong))
               .filter(_._5 > 70L)
               .filter(_._4=="huevos")

           }

               kafkaStream1.foreachRDD{line=>
                 almacenesDestinoRDD=line.map(_.split(","))
                   .map(attr=>(attr(0),attr(1).toDouble,attr(2).toDouble,attr(3),attr(4).toLong))
                   .filter(_._5 < 40L)
                   .filter(_._4=="huevos")
               }

            kafkaStream2.foreachRDD{ln=>

               val sqlC=new SQLContext(sc)
               import  sqlC.implicits._
               val ruta=ln.map(_.split(","))
                 .map(attr=>(attr(0),attr(1).toDouble,attr(2).toDouble,attr(3)))
                 .filter(_._4=="disponible")

               val rutaq=ruta.map(x=>(x._1,x._2,x._3))
           if(ruta.count()>0) {
          val df =  almacenesOrigenRDD.cartesian(rutaq)
               .map(x => (x._1._1,x._1._2,x._1._3, x._2._1, math.abs(x._1._2 - x._2._2) + math.abs(x._1._3 - x._2._3), x._2._2, x._2._3, x._1._4, x._1._5, 30, x._1._5 - 30))
               .cartesian(almacenesDestinoRDD).map(x=>(x._1._1,x._1._2,x._1._3,x._1._4,x._1._5,x._1._6,x._1._7,x._1._8,x._1._9,x._1._10,x._1._11,x._2._1.toString,x._2._2,x._2._3,x._2._4,x._2._5,math.abs(x._1._2-x._2._2)+math.abs(x._1._3-x._2._3))).filter(x=>(x._1.toString!=x._12.toString))
               .toDF("id_almacen_origen","almacen_origen_x","almacen_origen_y", "id_camion", "distancia", "camion_x", "camion_y", "referencia", "cantidad_origen", "cantidad_remision", "saldo_almacen_origen","id_almacen_destino","almacen_destino_x","almacen_destino_y","almacen_destino_referencia","almacen_destino_cantidad","distancia_almacen_destino")

             df.show()
           MongoSpark.save(df, writeConfig)

           }
             }


   kafkaStream1.print()



    ssc.start()
  ssc.remember(Seconds(60l))
   ssc.awaitTermination()
  }


}
