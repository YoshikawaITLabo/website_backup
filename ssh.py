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

def main():
    # 一時テーブルを空にする
    params=[]
    sql = "DELETE FROM `templist`;"
    non_select_state(sql,params)
    
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
        HOSTNAME = item[0]
        PORT = item[1]
        USER = item[2]
        PASSWORD = item[3]
        PRIVATE_KEY_PATH = item[4] + item[5]
        BKDIR = item[6]
        WRDIR = item[7]
        #後でNAS情報を追加

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

    #取得済みファイル一覧から、2日以内の日付を取得。一時取得テーブルに同名・同日付のファイルがあれば、削除する

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

if __name__ == "__main__":
    main()