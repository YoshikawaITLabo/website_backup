import mysql.connector
import paramiko
import datetime
import shutil
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import padding as sym_padding
import os
import mysql.connector
import base64

#グローバル変数
HOSTNAME = None
PORT = None
USER = None
PRIVATE_KEY_PATH = None
PASSWORD = None
BKDIR = None
WRDIR = None
STORAGE = None
cnx = None

SALT = None
KEY = None

def dbconnect():
    conn = mysql.connector.connect(
        host="localhost",
        user="root",
        password="ghcrxw7k",
        db='webbackup',
        charset='utf8'       
    )
    return conn

def select_state(sql,params):
    #SELECT文の戻り値があるSQLの実行。パラメータ付きも可
    conn = dbconnect()
    cursor = conn.cursor()
    cursor.execute(sql, params)
    result=cursor.fetchall()
    conn.close()
    
    return result

def non_select_state(sql,params):
    #INSERT DELETE UPDATEなど値が戻らないSQLの実行。パラメータ付きも可
    conn = dbconnect()
    cursor = conn.cursor()
    cursor.execute(sql,params)
    conn.commit()
    conn.close()

def sshhost(hostname,port,user,private_key_fn,passphrase):
    #取得したホストに接続し、ファイル名などの情報を一時テーブルに保管
    client = paramiko.SSHClient()
    client.load_system_host_keys()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    # 秘密鍵を読み込む
    private_key = paramiko.Ed25519Key(filename=private_key_fn, password=passphrase)

    # ホスト名を使用して接続
    client.connect(hostname, port=port, username=user, pkey=private_key)

    return client


def oldFilemove(sendfn,recefn):
    #テンポラリテーブルのファイル名を取得
    sql = "SELECT "
    sql += " `FileName`"
    sql += ",`FileSize`"
    sql += " FROM"
    sql += "`templist`"
    sql += " ORDER BY"
    sql += " CreationDate"
    sql += ";"

    params=[]
    result = select_state(sql,params)

    for item in result:
        
        shutil.copy2(sendfn + item[0], recefn)

        os.remove(sendfn + item[0])

    # 移動が完了したので、一時テーブルを空にする
    params=[]
    sql = "DELETE FROM `templist`;"
    non_select_state(sql,params)

def getfileList(client,bkdir):
    
    sftp_connection = client.open_sftp()
    sftp_connection.chdir(bkdir)

     # ファイルリストを取得。DBの一時ファイルに保管
    files = sftp_connection.listdir_attr()
    for rfile in files:
        if rfile.st_size > 0:

            params = [rfile.filename, rfile.st_size, datetime.datetime.fromtimestamp(rfile.st_mtime)]
            placeholders = ', '.join(['%s'] * len(params))

            sql = "INSERT INTO `templist`"
            sql += "("
            sql += " `FileName`"
            sql += ",`FileSize`"
            sql += ",`CreationDate`"
            sql += ") VALUES ("
            sql += placeholders
            sql += ");"

            non_select_state(sql,params)
 
    sftp_connection.close()

def duplication():
    #重複したデータを除去するため、取得済みファイル一覧から、2日以内の日付を取得。一時取得テーブルに同名・同日付のファイルがあれば、削除する
    two_days_ago = datetime.datetime.now() - datetime.timedelta(days=2)
    params = [two_days_ago]
    
    sql = "SELECT"
    sql += " `FileName`"
    sql += ",`FileSize`"
    sql += ",`CreationDate`"
    sql += " FROM"
    sql += " `finishedlist`"
    sql += "WHERE"
    sql += " `CreationDate`;"
    sql += " > %s"
    
    result = select_state(sql,params)

    for item in result:
        FILENAME , FILESIZE , CREADATE = item
        params = [FILENAME , FILESIZE , CREADATE]

        sql = "DELETE"
        sql += " FROM"
        sql += " `templist`"
        sql += " WHERE"
        sql += " `FileName` =  %s"
        sql += " AND"
        sql += " `FileSize` =  %s"
        sql += " AND"
        sql += " `CreationDate` = %s"
        sql += ";"

        non_select_state(sql,params)

def faileget(client):
    #ファイルを取得するため、一覧を取得
    sql = "SELECT"
    sql += " `FileName`"
    sql += ",`FileSize`"
    sql += ",`CreationDate`"
    sql += " FROM"
    sql += " `templist`;"

    params=[]
    result = select_state(sql,params)
    
    for item in result:

        FILENAME , FILESIZE , CREADATE = item
        params = [FILENAME , FILESIZE , CREADATE]
        placeholders = ', '.join(['%s'] * len(params))

        sftp_connection = client.open_sftp()

        sftp_connection.get(BKDIR + FILENAME, WRDIR + FILENAME)
        sftp_connection.close()
        
        #NASの保存域にファイルをコピー
        #shutil.copyfile(WRDIR + FILENAME, '/mnt/rasp_nas/ras_backup/' + FILENAME)

        sql = "INSERT INTO `finishedlist`"
        sql += "("
        sql += " `FileName`"
        sql += ",`FileSize`"
        sql += ",`CreationDate`"
        sql += ") VALUES ("
        sql += placeholders
        sql += ");"

        non_select_state(sql,params)

    client.close()

def main():

    #テキストファイルからソルトとキーを取り出す
    f = open('/home/kazuhiro/.ssh/salt_key.txt', 'r', encoding='UTF-8')

    line = f.readline()
    global SALT
    SALT= base64.b64decode(line.replace('s:',''))
    line = f.readline()
    global KEY
    KEY= base64.b64decode(line.replace('k:',''))

    f.close()
    
    #取得するホストの情報をデータベースから取得
    sql = "SELECT"
    sql += " `hostname`"
    sql += ",`port`"
    sql += ",`user`"
    sql += ",`passphrase`"
    sql += ",`keysfolder`"
    sql += ",`keyfile`"
    sql += ",`copy_source`"
    sql += ",`Copy_to`"
    sql += ",`Storage`"
    sql += " FROM"
    sql += " `hostlist`;"

    params=[]
    result = select_state(sql,params)

    for item in result:
        global HOSTNAME
        HOSTNAME = item[0]

        global PORT
        PORT = item[1]

        cipher3 = Cipher(algorithms.AES(KEY), modes.CBC(SALT), backend=default_backend())
        decryptor1 = cipher3.decryptor()
        coltext1 = item[2]
        dccoltext1 = base64.b64decode(coltext1)
        decrypted_padded_data1 = decryptor1.update(dccoltext1) + decryptor1.finalize()
        unpadder1 = sym_padding.PKCS7(128).unpadder()
        decrypted_data1 = unpadder1.update(decrypted_padded_data1) + unpadder1.finalize()
        global USER 
        USER = decrypted_data1.decode()

        cipher4 = Cipher(algorithms.AES(KEY), modes.CBC(SALT), backend=default_backend())
        decryptor2 = cipher4.decryptor()
        coltext2 = item[3]
        dccoltext2 = base64.b64decode(coltext2)
        decrypted_padded_data2 = decryptor2.update(dccoltext2) + decryptor2.finalize()
        unpadder2 = sym_padding.PKCS7(128).unpadder()
        decrypted_data2 = unpadder2.update(decrypted_padded_data2) + unpadder2.finalize()
        global PASSWORD
        PASSWORD = decrypted_data2.decode()

        global PRIVATE_KEY_PATH
        PRIVATE_KEY_PATH = item[4] + item[5]
        global BKDIR 
        BKDIR = item[6]

        global WRDIR 
        WRDIR = item[7]

        global STORAGE 
        STORAGE = item[8]

        #旧データ削除
        oldFilemove(WRDIR,STORAGE)

        #SSHクライアントに接続
        client = sshhost(HOSTNAME,PORT,USER,PRIVATE_KEY_PATH,PASSWORD)

        #取得したホストに接続し、ファイル名などの情報を一時テーブルに保管
        getfileList(client,BKDIR)

        #重複したファイル名を除去
        duplication()

        #ファイルをゲット
        faileget(client)


if __name__ == "__main__":
    main()