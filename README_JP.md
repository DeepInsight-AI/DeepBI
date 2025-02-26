<h1 align="center">DeepBI</h1>

<div align="center">

DeepBIは、AIネイティブのデータ分析プラットフォームです。DeepBIは、大規模言語モデルの力を活用して、あらゆるデータソースからデータを探索、クエリ、可視化、共有します。ユーザーはDeepBIを使用してデータの洞察を得て、データ駆動型の意思決定を行うことができます。

</div>

<div align="center">

  言語： 日本語 [English](README.md) [中文](README_CN.md)<br>
開発者：dev@deepbi.com  ビジネス：hi@deepbi.com

  <div style="display: flex; align-items: center;">

DeepBIが役立つと思われる場合は、こちらの<a style="display: flex; align-items: center;margin:0px 6px" target="_blank" href='https://github.com/DeepInsight-AI/DeepBI'>リンク</a>をクリックして、右上の⭐ StarとForkをお願いします。あなたのサポートが、DeepBIをより良くするための最大の原動力です。

  </div>
</div>

## ビデオ例

https://github.com/DeepInsight-AI/DeepBI/assets/151519374/d1effbe1-5c11-4c77-86ef-e01b1ea7f2f6

## ユーザーマニュアル
[DeepBIユーザーマニュアル](client/app/assets/images/jp/user_manual_jp.md)

## ✨ 特徴

1 会話型データ分析：ユーザーは対話を通じて任意のデータ結果と分析結果を得ることができます。\
2 会話型クエリ生成：対話を通じて永続的なクエリと可視化を生成します。\
3 ダッシュボード：永続的な可視化をダッシュボードに組み立てます。\
4 自動データ分析レポート（開発中）：ユーザーの指示に従ってデータ分析レポートを自動的に完了します。\
5 複数のデータソースをサポート、MySQL、PostgreSQL、Doris、StarRocks、CSV/Excelなどをサポート。\
6 マルチプラットフォームサポート、Windows-WSL、Windows、Linux、Macをサポート。\
7 国際化、英語、中国語、日本語をサポート。

## 🚀 サポートされているデータベース

DeepBIがサポートするデータベース接続は次のとおりです：
- MySQL
- PostgreSQL
- csv/Excelインポート
- Doris
- StarRocks
- MongoDB

## 📦 Windows exe インストール
- 最新バージョンの```window_install_exe_JP.zip```を<a href="https://github.com/DeepInsight-AI/DeepBI/releases">こちらからダウンロード</a>します。現在のテストはWin10とWin11をサポートしています。
- zipファイルを解凍し、.exeファイルをダブルクリックしてDeepBIを実行します。
- ローカルインストールの説明 [インストール exe](README_window_jp.md)

## 📦 Dockerビルド

- ローカル環境にはdockerとdocker-composeが必要です。<br>
- [Dockerのインストール](Docker_install.md)
- プロジェクトファイルをgitでダウンロードします：``` git clone https://github.com/DeepInsight-AI/DeepBI.git ``` <br>
  またはzipファイルを直接ダウンロードして解凍します。<br>
  ![download.png](user_manual/jp/img/download.png)

- プロジェクトディレクトリに入ります：``` cd DeepBI ```
- 直接``` ./Install.sh ```を実行します
- デフォルトポート：8338 8339
- Webアクセス：http://ip:8338
#### DeepBI dockerコマンド
- プロジェクトDeepBIディレクトリに入ります：
```
    docker-compose start # DeepBIサービスを開始
    docker-compose stop # DeepBIサービスを停止
    docker-compose ps # DeepBIサービスの状態を確認
```
- もし... PermissionError ... 'または' Permission denied'が表示された場合は、コマンドを実行する前に'sudo'を追加してください
```
    sudo docker-compose start # DeepBIサービスを開始
    sudo docker-compose stop # DeepBIサービスを停止
    sudo docker-compose ps # DeepBIサービスの状態を確認
```

## Ubuntuビルド
直接Ubuntuシステムにインストールするには、redis、postgresql、python3.8.17環境をインストールする必要があります。

- Redisは127.0.0.1のパスワードなしコマンドラインで直接アクセスできます。
- pythonバージョン3.8.xが必要です
- pyenv codaなどの仮想環境の使用をお勧めします
- postgresqlはpostgresql-16バージョンをインストールする必要があります

- コマンドでDeepBIコードをダウンロードします

```
git clone https://github.com/DeepInsight-AI/DeepBI.git
```
ダウンロードに失敗した場合はプロトコルを置き換えて、次のコードを実行します
```
git clone http://github.com/DeepInsight-AI/DeepBI.git
 ```

- 直接```. ubuntu_install.sh```を実行します（ここではsh xxxではなく. ubuntu_install.shを実行する必要があることに注意してください。python仮想環境を実行する必要があるためです）
- デフォルトポートは8338と8339です
- Webアクセス：http://ip:8338

## お問い合わせ

<a><img src="https://github.com/user-attachments/assets/21d879ea-151d-48c2-904d-ccdb68d56878" width="40%"/></a>

## 📑 その他
- Mac OS 12.7/13.X /14.1.1、Ubuntu 20.04/22.04、Windows11 WSL 22.04でテスト済みです。
- Windows 10はWSLをインストールするためにバージョン22H2以上が必要です
- サーバーの最小メモリ要件は1コア2Gメモリで、2コア4Gメモリ以上を推奨します
- 質問がある場合は、dev@deepbi.comまでお問い合わせください
- <a href="https://github.com/DeepInsight-AI/DeepBI/issues">Issue</a>

