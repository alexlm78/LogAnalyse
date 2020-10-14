#!/usr/bin/python3

import pysftp, shutil, ftplib, os
from datetime import datetime, timedelta
from loadLog import loadV9, loadV6, delCurrent

def Broker6():
    server = "192.168.136.84"
    user = "ggaitan"
    password = "Cl4r0Bu$+2020"
    source = "./"
    destination = "data/"
    interval = 0.05

    ftp = ftplib.FTP(server)
    ftp.login(user, password)

    ftp.cwd(source)
    filelist = ftp.nlst()

    file = 'mqtorpg.log'
    nfile ='mqtorpg.log_'+ datetime.today().strftime('%Y.%m.%d')
    ftp.retrbinary("RETR "+file, open(file, "wb").write)
    dst_file = os.path.join(destination, file)
    if os.path.exists(dst_file):
        os.remove(dst_file)
    shutil.move(file, destination)
    #os.rename('data/'+file, 'data/'+nfile)
    ftp.quit()

    loadV6(destination+file)

def Broker9():
    sHost = '192.168.8.34'
    sUser = 'acamey'
    sPass = 'Bu$ftp+2020'

    cnopts = pysftp.CnOpts()
    cnopts.hostkeys = None

    ayer = datetime.today() - timedelta(days=1)
    Files = ['ConsultaSaldo.log', 'PagoTelefonico.log']

    with pysftp.Connection(host=sHost, username=sUser, password=sPass, cnopts=cnopts) as sftp:
        for fil in Files:
            remoteFilePath = './'+fil
            localFilePath = 'data/'+fil
            sftp.get(remoteFilePath, localFilePath)
    
    delCurrent()
    for fil in Files:
        loadV9('data/'+fil)

if __name__ == "__main__":
    Broker9()
