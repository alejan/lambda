import threading, time
import os, ssl

from kafka.consumer.group import KafkaConsumer

class Consumer(object):

    def iniciar(self):
        ssl_context = ssl.SSLContext(ssl.PROTOCOL_SSLv23)

        ssl_context.verify_mode = ssl.CERT_REQUIRED
        ssl_context.check_hostname = True
        ssl_context.load_verify_locations("../conf/ca.pem")
        ssl_context.load_cert_chain("../conf/cert.pem", "../conf/key.pem")

        brokers = "steamer-01.srvs.cloudkafka.com:9093,steamer-02.srvs.cloudkafka.com:9093,steamer-03.srvs.cloudkafka.com:9093".split(
            ',')
        consumer = KafkaConsumer('zfey-proyecto-kafka',
                                 bootstrap_servers=brokers,
                                 security_protocol='SSL',
                                 ssl_context=ssl_context)
        print "Iniciando Consumer Camion"
        for message in consumer:
            print ("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
                                                  message.offset, message.key,
                                                  message.value))


if __name__ == "__main__":
        Consumer().iniciar()