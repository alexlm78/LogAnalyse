#!/usr/bin/python3

import sys,os,time
import ftplib, pysftp 
import shutil
from datetime import datetime, timedelta
#from loadLog import loadIt

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
    os.rename('data/'+file, 'data/'+nfile)

    ftp.quit()

    #loadIt('data/'+nfile)

def Broker9():
    sHost = '192.168.8.34'
    sUser = 'acamey'
    sPass = 'Bu$ftp+2020'

    cnopts = pysftp.CnOpts()
    cnopts.hostkeys = None

    ayer = datetime.today() - timedelta(days=1)
    Files = ['ConsultaSaldo-'+ayer.strftime('%Y%m%d')+'.log', 'PagoTelefonico-'+ayer.strftime('%Y%m%d')+'.log']

    with pysftp.Connection(host=sHost, username=sUser, password=sPass, cnopts=cnopts) as sftp:
        for fil in Files:
            remoteFilePath = './backup/'+fil
            localFilePath = 'data/'+fil+'_'+datetime.today().strftime('%Y.%m.%d')
            print('getting: '+remoteFilePath)
            sftp.get(remoteFilePath, localFilePath)

if __name__ == "__main__":
    if int(sys.argv[1]) == 6:
        Broker6()
    if int(sys.argv[1]) == 9:
        Broker9()
