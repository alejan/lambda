import threading, time
import os, ssl

from kafka.consumer.group import KafkaConsumer

class Consumer(object):

    def iniciar(self):
        brokers = "localhost:9092".split(
            ',')
        consumer = KafkaConsumer('rutas','postcliente',
                                 bootstrap_servers=brokers)
        print "Iniciando Consumer"
        for message in consumer:
            print ("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
                                                  message.offset, message.key,
                                                  message.value))


if __name__ == "__main__":
        Consumer().iniciar()