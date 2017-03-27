import paho.mqtt.client as mqtt

import programa_kafka.ProducerCamion
from logica.camion import camion


class CamionMqtt_sub(object):
    c = camion()
    topico = ""
    producer_camion = programa_kafka.ProducerCamion.ProducerCamion()
    lista_brokers_kafka = "localhost:9092"
    topico_kafka = "ccp-camiones-topico"

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
        self.c.transformar_JSON(mensaje)
        self.producer_camion.producir(self.c.mensaje_json)
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
    topico = "/CCP/envio_camiones"

    suscriptor = CamionMqtt_sub()
    cliente = suscriptor.configurar_recibidos()
    suscriptor.conectar(cliente, usuarioCloudMqtt, pwdCloudMqtt, host, puerto)
    suscriptor.suscribir(cliente, topico)

