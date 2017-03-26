import threading, time
import os, ssl

from kafka.producer.kafka import KafkaProducer


class ProducerCamion(object):

    contexto_ssl = None
    brokers = []
    topico = ""
    producer = None

    def __init__(self):
        pass

    def configurar(self, lista_brokers, topico):
        self.brokers = lista_brokers.split(",")
        self.topico = topico

    def conectar(self):
        self.contexto_ssl = ssl.SSLContext(ssl.PROTOCOL_SSLv23)
        self.contexto_ssl.verify_mode = ssl.CERT_REQUIRED
        self.contexto_ssl.check_hostname = True
        self.contexto_ssl.load_verify_locations("../conf/ca.pem")
        self.contexto_ssl.load_cert_chain('../conf/cert.pem', '../conf/key.pem')
        self.producer = KafkaProducer(bootstrap_servers=self.brokers,
                                 security_protocol='SSL',
                                 ssl_context=self.contexto_ssl)
        print "Conectado Kafka"

    def producir(self, mensaje):
        future = self.producer.send(self.topico, mensaje)
        try:
            record_metadata = future.get(timeout=10)
        except Exception:
            pass

        print (record_metadata.topic)
        print (record_metadata.partition)
        print (record_metadata.offset)
