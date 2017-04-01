package main.scala.streaming


import com.mongodb.spark.{rdd, _}
import com.mongodb.spark.config.{MongoInputConfig, ReadConfig, WriteConfig}
import kafka.serializer.StringDecoder
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
  sc.setCheckpointDir("/tmp/checkpoint/")
  val batchDuration= Seconds(4)

  val ubicacion:Array[Array[String]]=Array(
    Array("00-0","4.678496643716877","-74.07829481595651"),
    Array("00-1","4.561383203074723","-74.15132568473086"),
    Array("00-2","4.593733549021873","-74.11726505905634"),
    Array("00-3","4.683061396012669","-74.13293980493451"),
    Array("00-4","4.561038925510342","-74.107038936024"),
    Array("00-5","4.675494739004833","-74.12092386365383"),
    Array("00-6","4.686843704367104","-74.05345382759107"),
    Array("00-7","4.56987032072677","-74.05347630586695"),
    Array("00-8","4.591389018634298","-74.16302861787831"),
    Array("00-9","4.6654491740763095","-74.10779380212816"),
    Array("00-10","4.614207982990985","-74.1408286136029"),
    Array("00-11","4.679352123399475","-74.09027149194253"),
    Array("00-12","4.5892907205822695","-74.14863726725777"),
    Array("00-13","4.653465590739137","-74.0718503016028"),
    Array("00-14","4.675819144333476","-74.08371294486871"),
    Array("00-15","4.626898484352682","-74.10720011022805"),
    Array("00-16","4.631381236529341","-74.05174245857285"),
    Array("00-17","4.659679407754971","-74.04808207079037"),
    Array("00-18","4.680224726420009","-74.1504071048683"),
    Array("00-19","4.602037526171144","-74.09554142593856"),
    Array("00-20","4.616460715993738","-74.09624156852178"),
    Array("00-21","4.564381011197297","-74.12018179957563"),
    Array("00-22","4.687506058570198","-74.15489919303968"),
    Array("00-23","4.596942303124986","-74.16224513685721"),
    Array("00-24","4.641078449423352","-74.05945081619406"),
    Array("00-25","4.577612344068298","-74.1007382040618"),
    Array("00-26","4.627182697459964","-74.14987072277337"),
    Array("00-27","4.578177636369638","-74.10975888924938"),
    Array("00-28","4.62428002548381","-74.1601920305535"),
    Array("00-29","4.663185057643341","-74.10000142988515"),
    Array("00-30","4.6655592311751635","-74.09888127679835"),
    Array("00-31","4.591223565411308","-74.15523508582136"),
    Array("00-32","4.677944076740379","-74.09773597186147"),
    Array("00-33","4.6847861408523865","-74.15098983742021"),
    Array("00-34","4.679165667940699","-74.15602732028664"),
    Array("00-35","4.6092449426949","-74.15211120810622"),
    Array("00-36","4.595798423328668","-74.108631534037"),
    Array("00-37","4.576043163143224","-74.13147850106029"),
    Array("00-38","4.643053861021931","-74.05006872359888"),
    Array("00-39","4.627998927105223","-74.09099029585873"),
    Array("00-40","4.680843706053154","-74.0948286135235"),
    Array("00-41","4.658352536357823","-74.08209449765242"),
    Array("00-42","4.619373661986909","-74.07925309935814"),
    Array("00-43","4.621086417935673","-74.12075084683336"),
    Array("00-44","4.640411813609373","-74.0463050248597"),
    Array("00-45","4.685043435965843","-74.11254432760992"),
    Array("00-46","4.642637790079524","-74.04825846129503"),
    Array("00-47","4.6601756704179556","-74.11594976818922"),
    Array("00-48","4.675467731622116","-74.09423620927788"),
    Array("00-49","4.563124248973617","-74.0537274522053"),
    Array("00-50","4.567943606952652","-74.11670934356525"),
    Array("00-51","4.617140649064314","-74.13348462458066"),
    Array("00-52","4.638573611336724","-74.08176513135363"),
    Array("00-53","4.607884740783043","-74.09627641179576"),
    Array("00-54","4.63884224433882","-74.06627390804172"),
    Array("00-55","4.607550837379998","-74.08723783946974"),
    Array("00-56","4.663824135685082","-74.09044812311238"),
    Array("00-57","4.665103309298551","-74.05653042388586"),
    Array("00-58","4.671224816039671","-74.07786212508172"),
    Array("00-59","4.606137586652535","-74.15925607117147"),
    Array("00-60","4.655525554049792","-74.11209304009174"),
    Array("00-61","4.624701962706897","-74.09076596153712"),
    Array("00-62","4.648305652993389","-74.05269358405194"),
    Array("00-63","4.59093721551682","-74.10060046290317"),
    Array("00-64","4.658082935033395","-74.12847851056851"),
    Array("00-65","4.674280933319767","-74.08898820318011"),
    Array("00-66","4.68882676634625","-74.15905203742685"),
    Array("00-67","4.63305340870894","-74.05247191596051"),
    Array("00-68","4.611694456625742","-74.1348376346003"),
    Array("00-69","4.594405060964532","-74.15940383776802"),
    Array("00-70","4.61913801158113","-74.04819820421832"),
    Array("00-71","4.644876696274844","-74.1628121524469"),
    Array("00-72","4.661508854603113","-74.14048594112694"),
    Array("00-73","4.5866073475351055","-74.07869815382954"),
    Array("00-74","4.596265655689664","-74.06507693219109"),
    Array("00-75","4.584547994039315","-74.04870170185264"),
    Array("00-76","4.590599351491678","-74.0921798891378"),
    Array("00-77","4.63457432304697","-74.06070785536909"),
    Array("00-78","4.579707288106875","-74.08289129782295"),
    Array("00-79","4.632240505217896","-74.12060453517486"),
    Array("00-80","4.662236613442472","-74.04623081098147"),
    Array("00-81","4.560706115989971","-74.07479356893029"),
    Array("00-82","4.559347297971171","-74.04805666235455"),
    Array("00-83","4.6236935080690795","-74.14146754279733"),
    Array("00-84","4.666442605361612","-74.1252575201569"),
    Array("00-85","4.605959381060581","-74.12503246645564"),
    Array("00-86","4.586566386843249","-74.14723833527609"),
    Array("00-87","4.689488769700453","-74.13876154057452"),
    Array("00-88","4.57413442593657","-74.05884149648922"),
    Array("00-89","4.684141460410316","-74.06770194819887"),
    Array("00-90","4.609058785556795","-74.05919483132605"),
    Array("00-91","4.610131615825218","-74.1545230099326"),
    Array("00-92","4.580157415496544","-74.12659889632154"),
    Array("00-93","4.57148045402784","-74.14906585480252"),
    Array("00-94","4.668784172749247","-74.06103735577878"),
    Array("00-95","4.558726968109542","-74.1470982416052"),
    Array("00-96","4.585037172237801","-74.08383826137917"),
    Array("00-97","4.5634840528555936","-74.0800571240162"),
    Array("00-98","4.608759476483176","-74.11547455206383"),
    Array("00-99","4.675557783864731","-74.09991814201764"))
  val ubicacionRDD=sc.parallelize(ubicacion)

  @transient  val ssc = new StreamingContext(sc, batchDuration)

  ssc.checkpoint("file:///usr/local/Cellar/kafka/checkpoint")

  val kafkaParams= Map(
    "metadata.broker.list"->"localhost:9092",
    "group.id"->"lambda",
    "auto.offset.reset"->"smallest"
  )
  val writeConfig = WriteConfig(Map("collection" -> "almacenesorig", "writeConcern.w" -> "majority"), Some(WriteConfig(sc)))

  val kafkaStream1=KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](
    ssc,kafkaParams,Set("postcliente")).map(_._2)

  val kafkaStream2=KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](
    ssc,kafkaParams,Set("rutas")).map(_._2)


  def main(args: Array[String]): Unit = {


  //  val sparkDocuments = sc.parallelize((1 to 10).map(i => Document.parse(s"{spark: $i}")))

  // MongoSpark.save(sparkDocuments, writeConfig)



    val tiendaOrigenDS = kafkaStream1.transform{line=>
      line.map(_.split(","))
        .map(attr=>(attr(0),attr(1).toDouble,attr(2).toDouble,attr(3),attr(4).toLong))
        .filter(_._5 > 70L)
        .filter(_._4=="huevos")

    }
    val rutaDS = kafkaStream2.transform{line=>
      line.map(_.split(","))
        .map(attr=>(attr(0),attr(1).toDouble,attr(2).toDouble,attr(3)))
        .filter(_._4=="disponible")


    }
   kafkaStream2.foreachRDD{ln=>

    val sqlC=new SQLContext(sc)

    import  sqlC.implicits._
    val ruta=ln.map(_.split(","))
      .map(attr=>(attr(0),attr(1).toDouble,attr(2).toDouble,attr(3)))
      .filter(_._4=="disponible")
    val ubica=ubicacionRDD.map(x=>(x(0),x(1).toDouble,x(2).toDouble))
val rutaq=ruta.map(x=>(x._1,x._2,x._3))
     ubica.toDF().show()
    if(ruta.count()>0){
     ruta.toDF().show()
      ubica.cartesian(rutaq)
        .map(x=>(x._1._1,x._2._1,math.abs(x._1._2-x._2._2)+math.abs(x._1._3-x._2._3))).toDF().show()

    }

   // ln.map(_._1).cartesian(ubica).toDF().show()

  }

/*kafkaStream1.foreachRDD{line=>
     val sqlC=new SQLContext(sc)
    import  sqlC.implicits._

      val tiendaDestino=line.flatMap(_.split(",")).toDF()
     // .map(attr=>(attr(0),attr(1).toDouble,attr(2).toDouble,attr(3),attr(4).toLong))
      //.filter(x=>(x._5 > 70L && x._4=="huevos")).toDF()

       //.map(i=> Document.parse(s"{id:${i._1}},{x:${i._2}},{y:${i._3}},{referencia:${i._4}},{qty:${i._5}}}"))
     MongoSpark.save(tiendaDestino,writeConfig)
    tiendaDestino.show()

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
*/
 //   rutaDS.print()
  // tiendaOrigenDS.print()



    ssc.start()

   ssc.awaitTermination()
  }


}
