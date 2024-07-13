・開発はRaspberry Piにて行いました。

・LOCAL環境にデータベースが必要です。
　sudo apt-get install mariadb-server
　でインストールしてください。

・データベースを作成してください。
　create database webbackup;

・テーブル名一覧
　hostlist
　templis
　finishedlist

・テーブル構造一覧
　・hostlist
    hostname	VARCHAR	ホスト名
    port	VARCHAR	使用ポート
    user	VARCHAR	ユーザー名
    passphrase	VARCHAR	パスフレーズ
    keysfolder	VARCHAR	キーファイル格納先
    keyfile	VARCHAR	キーファイル名
    copy_source	VARCHAR	ホスト上のファイル位置
    Copy_to	VARCHAR	LOCAL上のファイル位置
    Storage	VARCHAR	保存先のファイル位置(NASなど)
