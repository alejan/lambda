import paho.mqtt.client as mqtt

import programa_kafka.ProducerTienda
from logica.tienda import tienda


class TiendaMqtt_sub(object):
    t = tienda()
    topico = ""
    producer_camion = programa_kafka.ProducerTienda.ProducerTienda()
    lista_brokers_kafka = "192.168.200.201:9092"
    topico_kafka = "postcliente"

    def __init__(self):
        pass

    def on_connect(self, client, userdata, flags, rc):
        self.producer_camion.configurar(self.lista_brokers_kafka, self.topico_kafka)
        self.producer_camion.conectar()
        print("Conectado MQTT: " + mqtt.connack_string(rc))


    def on_disconnect(self, client, userdata, rc):
        if rc != 0:
            print("Se desconecto")

    def on_subscribe(self, client, userdata, mid, granted_qos):
        print("Suscrito a: "+self.topico)

    def on_message(self, client, userdata, msg):
        mensaje = str(msg.payload)
        self.t.transformar_trama_kafka(mensaje)
        self.producer_camion.producir(self.t.mensaje)
        print("Mensaje recibido: " + msg.topic + " " + mensaje)

    def configurar_recibidos(self):
        cliente = mqtt.Client()
        cliente.on_connect = self.on_connect
        cliente.on_disconnect = self.on_disconnect
        cliente.on_subscribe = self.on_subscribe
        cliente.on_message = self.on_message

        return cliente

    def conectar(self, cliente, usuario, passwd, host, puerto):
        cliente.username_pw_set(usuario, passwd)
        cliente.connect(host, puerto)

    def suscribir(self, cliente, topico):
        self.topico = topico
        cliente.subscribe(topico)
        cliente.loop_forever()

if __name__ == "__main__":
    usuarioCloudMqtt = "eorsbxtt"
    pwdCloudMqtt = "w-jUWASBEFgW"
    host = "m12.cloudmqtt.com"
    puerto = 14256
    topico = "/CCP/envio_tiendas"

    suscriptor = TiendaMqtt_sub()
    cliente = suscriptor.configurar_recibidos()
    suscriptor.conectar(cliente, usuarioCloudMqtt, pwdCloudMqtt, host, puerto)
    suscriptor.suscribir(cliente, topico)

