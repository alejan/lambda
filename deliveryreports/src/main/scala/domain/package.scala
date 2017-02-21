/**
  * Created by laura on 20-Feb-17.
  */
package object domain {
case class Pedido (
                     id: Long,
                     producto: String,
                     cantidad: Long,
                     cliente: String,
                     fecha: String,
                     prioridad: String,
                     valor: Long
                   )

  case class Entrega (
                       pedido_id: String,
                       ciudad: String,
                       direccion: String,
                       estado_entrega: String,
                       conductor: String,
                       caracteristicas_de_transporte: String,
                       posicion_de_carga: String,
                       sitio_de_embarque: String,
                       tipo_de_embalaje: String,
                       tratamiento_de_carga: String,
                       observaciones_cliente: String
                     )
}
