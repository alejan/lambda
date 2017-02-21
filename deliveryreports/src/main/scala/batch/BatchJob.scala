package batch

import java.lang.management.ManagementFactory

import domain.{Entrega, Pedido}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SaveMode}
/**
  * Created by cmercado on 2/19/17.
  */
object BatchJob {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("CCP delivery")


    if (ManagementFactory.getRuntimeMXBean.getInputArguments.toString.contains("IntelliJ IDEA")) {
      System.setProperty("hadoop.home.dir", "K:\\UniAndes\\winutils")
      conf.setMaster("local[*]")
    }

    val sc = new SparkContext(conf)
    val sQLContext = new SQLContext(sc)

    val sourceFileEntrega = "file:///vagrant/data/entregas.csv"
    val inputEntrega = sc.textFile(sourceFileEntrega)

    val sourceFilePedido = "file:///vagrant/data/pedidos.csv"
    val inputPedido = sc.textFile(sourceFilePedido)

    import org.apache.spark.sql.functions._
    import sQLContext.implicits._

    val inputRDDEntregas = inputEntrega.flatMap { line =>
      val record = line.split(",")
      if(record.length == 11)
        Some(Entrega(record(0), record(1), record(2), record(3), record(4), record(5), record(6), record(7), record(8), record(9), record(10)))
      else
        None
    }

    val inputDFPedidos = inputPedido.flatMap { line =>
      val record = line.split(",")
      if(record.length == 7)
        Some(Pedido(record(0).toLong, record(1), record(2).toLong, record(3), record(4), record(5), record(6).toLong))
      else
        None
    }.toDF()


    val keyedByCity = inputRDDEntregas.keyBy(a => (a.ciudad, a.estado_entrega)).cache()

    val stateByCity = keyedByCity.mapValues( a => a.estado_entrega ).distinct().countByKey()


    stateByCity.foreach(println)


    val inputDFEntregas = inputEntrega.flatMap { line =>
      val record = line.split(",")
      if(record.length == 11)
        Some(Entrega(record(0), record(1), record(2), record(3), record(4), record(5), record(6), record(7), record(8), record(9), record(10)))
      else
        None
    }.toDF()

    val dfEntregasByCity = inputDFEntregas.select(
      inputDFEntregas("pedido_id"),
      inputDFEntregas("ciudad"),
      inputDFEntregas("direccion"),
      inputDFEntregas("estado_entrega"),
      inputDFEntregas("conductor"),
      inputDFEntregas("caracteristicas_de_transporte"),
      inputDFEntregas("posicion_de_carga"),
      inputDFEntregas("sitio_de_embarque"),
      inputDFEntregas("tipo_de_embalaje"),
      inputDFEntregas("tratamiento_de_carga"),
      inputDFEntregas("observaciones_cliente")
    ).cache()

    dfEntregasByCity.registerTempTable("entrega")
    val entregaPorCiudad = sQLContext.sql(
      """SELECT ciudad, estado_entrega, COUNT(DISTINCT pedido_id) entregasPorEstado
        |FROM entrega GROUP BY ciudad, estado_entrega
      """.stripMargin).cache()
    entregaPorCiudad.printSchema()

    entregaPorCiudad.foreach(println)

    entregaPorCiudad.registerTempTable("estadoentregaPorCiudad")

    entregaPorCiudad.write.mode(SaveMode.Append).parquet("hdfs://lambda-pluralsight:9000/lambda/batch1")

  }
}
