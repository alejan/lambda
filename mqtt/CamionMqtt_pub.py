import paho.mqtt.client as mqtt

from logica.camion import camion
from logica.utilidades import utilidades


class CamionMqtt_pub(object):
    c = camion()

    def __init__(self):
        pass

    def on_connect(self, client, userdata, flags, rc):
        print("Conectado: " + mqtt.connack_string(rc))

    def on_disconnect(self, client, userdata, rc):
        if rc != 0:
            print("Se desconecto")

    def on_publish(self, client, userdata, mid):
        print("Mensaje publicado: " + self.c.mensaje)

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

    def publicar(self, cliente, topico, env_camion):
        self.c = env_camion
        cliente.publish(topico, self.c.mensaje)

if __name__ == "__main__":
    usuarioCloudMqtt = "eorsbxtt"
    pwdCloudMqtt = "w-jUWASBEFgW"
    host = "m12.cloudmqtt.com"
    puerto = 14256
    tiempo_permanencia = 60
    topico = "/CCP/envio_camiones"

    publicador = CamionMqtt_pub()
    cliente_camion = publicador.configurar_envios()
    publicador.conectar(cliente_camion, usuarioCloudMqtt, pwdCloudMqtt, host, puerto, tiempo_permanencia)
    for i in range(1,2):
        envio_camion = camion()
        envio_camion.id = i
        coordenada = utilidades.obtenerXYCamion()
        envio_camion.x = coordenada[0]
        envio_camion.y = coordenada[1]
        envio_camion.esta_disponible = utilidades.obtenerDisponibilidad()
        envio_camion.armar_trama()
        publicador.publicar(cliente_camion, topico, envio_camion)
