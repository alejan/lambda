class tienda:
    id = 0
    x = 0.0
    y = 0.0
    referencia = ""
    cantidad=0
    mensaje = ""
    mensaje_kafka = ""

    def __init__(self):
        pass

    def armar_trama(self):
        self.mensaje = "id="+str(self.id) + ";x=" + str(self.x) + ";y=" + str(self.y) + ";referencia=" + str(self.referencia) + ";cantidad=" + str(self.cantidad)

    def transformar_trama_kafka(self, tienda_str):
        tienda_split = tienda_str.split(";")
        self.mensaje_kafka = ""
        for cadena in tienda_split:
            atributo = cadena.split("=")
            if atributo[0] == "id":
                self.mensaje_kafka += "00-"+atributo[1].lower()
            else:
                self.mensaje_kafka += atributo[1].lower()
            self.mensaje_kafka += ","
        self.mensaje = self.mensaje_kafka[:-1]