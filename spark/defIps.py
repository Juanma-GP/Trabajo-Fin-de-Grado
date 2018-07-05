class DefIps:
    def parse(self,line):
        lineas=line.split(' ')
        self.ip=lineas[0]
        self.fecha=lineas[3]
        self.archivo=lineas[6]
        self.exito=lineas[8]
        return self
    
    def __repr__(self):
        return "Maquina::%s; Hora::%s;, Codigo de exito::%s, Pagina::%s"%(self.ip, self.fecha, self.exito, self.archivo)
    
    def to_json(self):
        return '{"Maquina":"%s", "Fecha":"%s", "Archivo":"%s", "Exito":"%s"}'%(self.ip, self.fecha, self.archivo. self.exito
)

