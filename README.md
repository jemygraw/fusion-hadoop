# hdfs

```
<property>
    <name>fs.qiniu.access.key</name>
    <value>ACCESS KEY</value>
</property>
<property>
    <name>fs.qiniu.secret.key</name>
    <value>SECRET KEY</value>
</property>
```

使用方式：

```
$ hdfs dfs -ls fusion://img.abc.com/2017-05-22/
```

```
$ hdfs dfs -ls fusion://img.abc.com/2017-05-22/22/
```

```
$ hdfs dfs -ls fusion://img.abc.com/2017-05-22/22/part-00000.gz
```


# spark

```
spark-shell --jars $HADOOP_HOME/qiniu-lib/fusion-hadoop-1.0.0.jar
```

```
sc.setLogLevel("INFO")
sc.hadoopConfiguration.set("fs.qiniu.access.key","ACCESS KEY")
sc.hadoopConfiguration.set("fs.qiniu.secret.key","SECRET KEY")
val textFile=sc.textFile("fusion://if-pbl.qiniudn.com/2017-05-22/16/part-00000.gz")
textFile.count()
```
