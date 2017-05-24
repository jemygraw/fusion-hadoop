# spark

```
spark-shell --jars $HADOOP_HOME/qiniu-lib/fusion-hadoop-1.0.0.jar
```

```
sc.setLogLevel("INFO")
sc.hadoopConfiguration.set("fs.qiniu.access.key","access key")
sc.hadoopConfiguration.set("fs.qiniu.secret.key","secret key")
val textFile=sc.textFile("fusion://if-pbl.qiniudn.com/2017-05-22/16/part-00000.gz")
textFile.count()
```
