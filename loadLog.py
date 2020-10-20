#!/usr/bin/python3

import sys, os
from datetime import datetime, timedelta
from Consulta import Consulta
import cx_Oracle

# Loading cx_Oracle from Oracle instantClient
try:
    cx_Oracle.init_oracle_client(lib_dir=os.getenv('ORACLE_CLIENT'))
except Exception as err:
    print("Whoops!")
    print(err)
    sys.exit(1)

# Load data for broker version 9
def loadV9(argv):
    with open(argv, 'r', errors='replace') as fp:
        Lines = fp.readlines()

        conn = cx_Oracle.connect('PMAL/Pm41$_2018f@(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST=oracleprd03-scan)(PORT=3875))(ADDRESS=(PROTOCOL=TCP)(HOST=172.17.225.174)(PORT=3876)))(CONNECT_DATA=(SERVER=dedicated)(SERVICE_NAME=PISAPRC)))')
        cur = conn.cursor()

        for line in Lines:
            tmp = line.split(' ', 3)

            sHeader = tmp[3][0:99].strip()
            sDetail = tmp[3][100:].strip()

            # Getting Balance (Request)
            if len(sDetail) == 30 and sDetail[10:13]=='060' and len(sHeader)>0:
                CC = Consulta()
                CC.req(sDetail)
                #print(sDetail)
                statement = 'insert into la_consultas (header, telefono, transaccion, banco, fecconsul, horaconsul, factura, fecfact, saldo, status) VALUES ( :2, :3, :4, :5, :6, :7, :8, :9, :10, :11 )'
                cur.execute(statement, (sHeader, CC.Telefono, CC.Trans, CC.Banco, CC.Fecha, CC.Hora, '', 0, 0, ' '))
                conn.commit()
            
            # Getting Balance (Response)
            if len(sDetail) == 81 and sDetail[10:13]=='061' and len(sHeader)>0:
                CC = Consulta()
                CC.res(sDetail)
                #print(sDetail)
                statement = 'update la_consultas set saldo= :2, factura=:3, fecfact=:4, status=:5 where header=:6 and telefono=:7'
                cur.execute(statement, (CC.Saldo, CC.NumFact, CC.FecFact, CC.Estatus, sHeader, CC.Telefono))
                conn.commit()
            
            # Doing Payment (request)
            if len(sDetail) == 102 and sDetail[10:13]=='062' and len(sHeader)>0:
                Tel = sDetail[0:10]
                Tran = sDetail[10:13]
                fecha = sDetail[13:21]
                hora = sDetail[21:27]
                banco = sDetail[27:38]
                total = sDetail[89:102]
                
                statement = 'insert into la_Pagos (header, telefono, trasaccion, fecha, hora, banco, monto, estatus) values  ( :2, :3, :4, :5, :6, :7, :8, :9 )'
                cur.execute(statement, (sHeader, Tel, Tran, fecha, hora, banco, total, ' '))
                conn.commit()
            
            # Doing Payment (response)
            if len(sDetail) == 10 and sDetail[3:6]=='063' and len(sHeader)>0:
                codbank = sDetail[0:3]
                tran = sDetail[3:6]
                estatus = sDetail[6:10]
                statement = 'update la_Pagos set estatus=:2 where header=:3'
                cur.execute(statement, (estatus, sHeader))
                conn.commit()
            
            if len(sDetail) == 63 and sDetail[10:13]=='070' and len(sHeader)>0:
                print('Consulta de Saldos por Internet [RQ]')
            
            if len(sDetail) == 84 and sDetail[10:13]=='071' and len(sHeader)>0:
                print('Consulta de Saldos por Internet [RS]')
            
            if len(sDetail) == 55 and sDetail[10:13]=='072' and len(sHeader)>0:
                print('Pago Telefónico por Internet [RQ]')
            
            if len(sDetail) == 10 and sDetail[3:6]=='073' and len(sHeader)>0:
                print('Pago Telefónico por Internet [RS]')
        
        cur.close()

# Load data for broker version 6
def loadV6(argv):
    # set number variables.
    aBalance = [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]
    aConsult = [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]
    aPayment = [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]
    vDate = ''
    
    # Reading the file...
    with open(argv, 'r', errors='replace') as fp:
        Lines = fp.readlines()
        # For every line on file.
        for line in Lines:
            tmp = line.split(' ', 3)    # Splitting....
            
            vDate = tmp[0]  # Date
            vTime = tmp[1]  # Time
            vType = tmp[2]  # Trasnsaction type
            vXML = tmp[3]   # XML string.

            iHour = int(vTime[:2])
            if vXML.find('Buffer')==-1:
                if vXML.find('SB_CNSLTA_SALDO_LTEL_FIJ_001_Request')>0:     # Balance
                    aBalance[iHour] += 1
                if vXML.find('SB_PAGO_LTEL_PISA_001_Request')>0:            # Status
                    aPayment[iHour] += 1
                if vXML.find('SB_CNSLTA_EST_LTEL_PISA_001_Request')>0:      # Payment
                    aConsult[iHour] += 1
    
    # Store the completed data.
    conn = cx_Oracle.connect('PMAL/Pm41$_2018f@(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST=oracleprd03-scan)(PORT=3875))(ADDRESS=(PROTOCOL=TCP)(HOST=172.17.225.174)(PORT=3876)))(CONNECT_DATA=(SERVER=dedicated)(SERVICE_NAME=PISAPRC)))')
    cur = conn.cursor()

    statement = 'delete from logAnalyse where la_date=:2'
    cur.execute(statement, datetime.strptime(vDate, '%Y-%m-%d').strftime('%Y%m%d'))
    conn.commit()

    statement = 'insert into logAnalyse(la_date, la_hour, la_consult, la_balance, la_payment) values ( :2, :3, :4, :5, :6)'
    # Insert data for every one of the 24 hours.
    for x in range(24):
        cur.execute(statement, (datetime.strptime(vDate, '%Y-%m-%d').strftime('%Y%m%d'), x, aConsult[x], aBalance[x], aPayment[x]))
    conn.commit()
    cur.close()
    
    # Closing files 
    fp.close()

def delCurrent():
    hoy = datetime.today()
    conn = cx_Oracle.connect('PMAL/Pm41$_2018f@(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST=oracleprd03-scan)(PORT=3875))(ADDRESS=(PROTOCOL=TCP)(HOST=172.17.225.174)(PORT=3876)))(CONNECT_DATA=(SERVER=dedicated)(SERVICE_NAME=PISAPRC)))')
    cur = conn.cursor()

    print('Deleting la_c')
    statement = 'delete from la_consultas where fecconsul='+hoy.strftime('%Y%m%d')
    cur.execute(statement)
    conn.commit()
    print('Deleting la_p')
    statement = 'delete from la_Pagos where fecha='+hoy.strftime('%Y%m%d')
    cur.execute(statement)
    conn.commit()  

    conn.close()

if __name__ == "__main__":
    hoy = datetime.today()
    ayer = hoy - timedelta(days=1)

    if int(sys.argv[1]) == 6:
        loadV6('data/mqtorpg.log_'+ayer.strftime('%Y.%m.%d'))
    if int(sys.argv[1]) == 9:
        Files = ['ConsultaSaldo-'+ayer.strftime('%Y%m%d')+'.log', 'PagoTelefonico-'+ayer.strftime('%Y%m%d')+'.log']
        for fil in Files:
            loadV9('data/'+fil)

