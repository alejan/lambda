import random
from threading import Thread

import paho.mqtt.client as mqtt
import time

from logica.tienda import tienda
from logica.utilidades import utilidades


class TiendaMqtt_pub(object):
    t = tienda()

    def __init__(self):
        pass

    def on_connect(self, client, userdata, flags, rc):
        print("Conectado: " + mqtt.connack_string(rc))

    def on_disconnect(self, client, userdata, rc):
        if rc != 0:
            print("Se desconecto")

    def on_publish(self, client, userdata, mid):
        print("Mensaje publicado: " + self.t.mensaje)

    def configurar_envios(self):
        cliente = mqtt.Client()
        # assign callbacks
        cliente.on_connect = self.on_connect
        cliente.on_disconnect = self.on_disconnect
        cliente.on_publish = self.on_publish
        return cliente

    def conectar(self, cliente, usuario, passwd, host, puerto, tiempo):
        cliente.username_pw_set(usuario, passwd)
        cliente.connect(host, puerto, tiempo)

    def publicar(self, cliente, topico, env_tienda):
        self.t = env_tienda
        cliente.publish(topico, self.t.mensaje)

if __name__ == "__main__":
    usuarioCloudMqtt = "eorsbxtt"
    pwdCloudMqtt = "w-jUWASBEFgW"
    host = "m12.cloudmqtt.com"
    puerto = 14256
    tiempo_permanencia = 60
    topico = "/CCP/envio_tiendas"

    publicador = TiendaMqtt_pub()
    cliente_tienda = publicador.configurar_envios()
    publicador.conectar(cliente_tienda, usuarioCloudMqtt, pwdCloudMqtt, host, puerto, tiempo_permanencia)
    id_tienda = 0
    referencias = utilidades.obtenerReferencias()
    cantidades = utilidades.obtenerCantidades()
    xs = []
    ys = []
    MAX = 100
    indice = 0
    while True:
        envio_tienda = tienda()
        if id_tienda == MAX:
            id_tienda = 0
            indice = 0

        if len(xs) != MAX:
            coordenada = utilidades.obtenerXYCamion()
            xs.append(coordenada[0])
            ys.append(coordenada[1])

        envio_tienda.x = xs[indice]
        envio_tienda.y = ys[indice]
        envio_tienda.id = id_tienda

        envio_tienda.referencia = referencias[random.randint(0, 4)]
        envio_tienda.cantidad = cantidades[random.randint(0, 9)]
        envio_tienda.armar_trama()
        id_tienda+=1
        indice+=1
        publicador.publicar(cliente_tienda, topico, envio_tienda)
        time.sleep(0.1)
