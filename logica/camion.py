from logica.utilidades import utilidades


class camion:
    id = 0
    x = 0.0
    y = 0.0
    esta_disponible = False
    mensaje = ""
    mensaje_json = ""
    mensaje_kafka = ""

    def __init__(self):
        id = 0
        x = 0.0
        y = 0.0
        esta_disponible = False
        mensaje = ""
        mensaje_json = ""

    def armar_trama(self):
        self.mensaje = "id="+str(self.id) + ";x=" + str(self.x) + ";y=" + str(self.y) + ";esta_disponible=" + str(self.esta_disponible)

    def transformar_JSON(self, camion_str):
        camion_split = camion_str.split(";")
        self.mensaje_json = "{"
        for cadena in camion_split:
            atributo = cadena.split("=")
            self.mensaje_json += "\""+atributo[0] + "\":" + atributo[1].lower() + ","
        self.mensaje_json = self.mensaje_json[:-1]
        self.mensaje_json += "}"

    def transformar_trama_kafka(self, camion_str):
        camion_split = camion_str.split(";")
        self.mensaje_kafka = ""
        for cadena in camion_split:
            atributo = cadena.split("=")
            if atributo[0] == "id":
                self.mensaje_kafka += "camion-"+atributo[1].lower()
            elif atributo[0] == "esta_disponible":
                self.mensaje_kafka += utilidades.obtenerDisponibilidadRes(atributo[1])
            else:
                self.mensaje_kafka += atributo[1].lower()
            self.mensaje_kafka += ","
        self.mensaje = self.mensaje_kafka[:-1]