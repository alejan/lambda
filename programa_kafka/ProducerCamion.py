import threading, time

from kafka.producer.kafka import KafkaProducer


class ProducerCamion(object):
    brokers = []
    topico = ""
    producer = None

    def __init__(self):
        pass

    def configurar(self, lista_brokers, topico):
        self.brokers = lista_brokers.split(",")
        self.topico = topico

    def conectar(self):
        self.producer = KafkaProducer(bootstrap_servers=self.brokers)
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
