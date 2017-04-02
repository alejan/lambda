import decimal
import random


class utilidades:
    MIN_X = 4.558327
    MIN_Y = -74.163206
    MAX_X = 4.689836
    MAX_Y = -74.046148
    coordenadas_existentes = []

    @staticmethod
    def obtenerXYCamion():
        encontro_coordenada = False
        while not encontro_coordenada:
            res_x = 0
            encontro_x = False
            while not encontro_x:
                respuesta = decimal.Decimal(random.randrange(10000000))/1000000
                if respuesta >= utilidades.MIN_X and respuesta <= utilidades.MAX_X:
                    res_x = respuesta
                    encontro_x = True
            res_y = 0
            encontro_y = False
            while not encontro_y:
                respuesta = -1*decimal.Decimal(random.randrange(100000000))/1000000
                if respuesta >= utilidades.MIN_Y and respuesta <= utilidades.MAX_Y:
                    res_y = respuesta
                    encontro_y = True


            coordenada = [res_x, res_y]
            if not utilidades.esta_coordenada(coordenada):
                utilidades.coordenadas_existentes.append(coordenada)
                encontro_coordenada = True
        return coordenada

    @staticmethod
    def esta_coordenada(coordenada):
        for c in utilidades.coordenadas_existentes:
            if coordenada[0] == c[0] and coordenada[1] == c[1]:
                return True

        return False

    @staticmethod
    def obtenerDisponibilidad():
        res = bool(random.getrandbits(1))
        return res

    @staticmethod
    def obtenerDisponibilidadRes(res):
        if res == 'True':
            return "disponible"
        return "no disponible"

    @staticmethod
    def obtenerReferencias():
        return ["queso", "pan", "leche", "huevos", "salchicha"]

    @staticmethod
    def obtenerCantidades():
        return [10, 20, 30, 40, 50, 60, 70 , 80, 90, 100]