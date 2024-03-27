import mysql.connector
import paramiko
import datetime
import shutil

HOSTNAME = None
PORT = None
USER = None
PRIVATE_KEY_PATH = None
PASSWORD = None
BKDIR = None
WRDIR = None
cnx = None

def dbconnect():
    conn = mysql.connector.connect(
        host="localhost",
        user="root",
        password="ghcrxw7k",
        db='webbackup',
        charset='utf8'       
    )
    return conn

def main():
    # MySQLに接続
    # カーソルを取得
    conn = dbconnect()
    cursor = conn.cursor()

    # 一時テーブルを空にする
    sql = "DELETE FROM `templist`;"
    cursor.execute(sql)
    conn.commit()
    cursor.close()
    conn.close()

    #取得するホストの情報をデータベースから取得    sql = "SELECT"
    sql = "SELECT"
    sql += " `hostname`"
    sql += ",`port`"
    sql += ",`user`"
    sql += ",`passphrase`"
    sql += ",`keysfolder`"
    sql += ",`keysfile`"
    sql += ",`sourcefolder`"
    sql += ",`destinationfolder`"
    sql += " FROM"
    sql += "`hostlist`;"

    con2 = dbconnect()
    cursor2 = con2.cursor()
    cursor2.execute(sql)
    result=cursor2.fetchall()

    for item in result:
        HOSTNAME = item[0]
        PORT = item[1]
        USER = item[2]
        PASSWORD = item[3]
        PRIVATE_KEY_PATH = item[4] + item[5]
        BKDIR = item[6]
        WRDIR = item[7]

    cursor2.close()
    con2.close()

    #取得したホストに接続する
    client = paramiko.SSHClient()
    client.load_system_host_keys()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    # 秘密鍵を読み込む
    private_key = paramiko.Ed25519Key(filename=PRIVATE_KEY_PATH, password=PASSWORD)

    # ホスト名を使用して接続
    client.connect(HOSTNAME, port=PORT, username=USER, pkey=private_key)
    
    sftp_connection = client.open_sftp()
    sftp_connection.chdir(BKDIR)

    #登録用DB
    con3 = dbconnect()

    # ファイルリストを取得。DBの一時ファイルに保管
    files = sftp_connection.listdir_attr()
    for rfile in files:
        if rfile.st_size > 0:

            sql = "INSERT INTO `templist`"
            sql += "("
            sql += " `FileName`"
            sql += ",`FileSize`"
            sql += ",`CreationDate`"
            sql += ") VALUES ("
            #sql += " '" + rfile.filename + "'"
            #sql += "," + str(rfile.st_size)
            #sql += ",'" + str(datetime.datetime.fromtimestamp(rfile.st_mtime)) + "'"
            sql += "%s , %s , %s"
            sql += ");"

            cursor3 = con3.cursor()
            cursor3.execute(sql,(rfile.filename , rfile.st_size , datetime.datetime.fromtimestamp(rfile.st_mtime)))
            con3.commit()
            cursor3.close()
            
            #print(rfile.filename + ":" + str(datetime.datetime.fromtimestamp(rfile.st_atime)))
            #print (sql)
    sftp_connection.close()
    con3.close()

    #重複ファイルを一覧から削除
    con4 = dbconnect()
    cursor4 = con4.cursor()

    #取得するホストの情報をデータベースから取得
    sql = "SELECT"
    sql += " `FileName`"
    sql += ",`FileSize`"
    sql += ",`CreationDate`"
    sql += " FROM"
    sql += " `finishedlist`"
    sql += "WHERE"
    sql += " `CreationDate`;"
    sql += " > %s"
    two_days_ago = datetime.datetime.now() - datetime.timedelta(days=2)
    cursor4.execute(sql, (two_days_ago,)) 
    result=cursor4.fetchall()

    con5 = dbconnect()

    for item in result:
        FILENAME , FILESIZE , CREADATE = item

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

        cursor5 = con5.cursor()
        cursor5.execute(sql,(FILENAME , FILESIZE, CREADATE,))
        con5.commit()
        cursor5.close()

    cursor4.close()

    #ファイルを取得するため、一覧を取得
    sql = "SELECT"
    sql += " `FileName`"
    sql += ",`FileSize`"
    sql += ",`CreationDate`"
    sql += " FROM"
    sql += " `templist`;"

    con6 = dbconnect()
    cursor6 = con6.cursor()
    cursor6.execute(sql)

    result=cursor6.fetchall()

    con7 = dbconnect()

    for item in result:
        FILENAME = item[0]
        FILESIZE = item[1]
        CREADATE = item[2]

        sftp_connection = client.open_sftp()
        #sftp_connection.chdir(BKDIR)

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
        sql += " '" + FILENAME + "'"
        sql += "," + str(FILESIZE)
        sql += ",'" + str(CREADATE) + "'"
        sql += ");"

        cursor7 = con7.cursor()
        cursor7.execute(sql)
        con7.commit()
        cursor7.close()

    client.close()

if __name__ == "__main__":
    main()