class Consulta:
    def req (self, detail):
        self.Telefono = int(detail[0:10])
        self.Trans = int(detail[10:13])
        self.Banco = detail[13:16]
        self.Fecha = detail[16:24]
        self.Hora = detail[24:30]

    def res (self, detail):
        self.Telefono = detail[0:10]
        self.Trans = detail[10:13]
        self.Banco = detail[13:16]
        self.Saldo = int(detail[16:29])/100
        self.FecFact = detail[29:37]
        self.NumFact = detail[37:47]
        self.Estatus = detail[77:81]

'''
 tel = sDetail[0:10]
                tran = sDetail[10:13]
                banco = sDetail[13:16]
                fecha = sDetail[16:24]
                hora = sDetail[24:30]
'''