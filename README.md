# hbase-crawler

A crawler parses html title, link, content and store them to hbase

## Usage

Fisrt, compile `HBaseCrawler.java` and get the output `jar` file, then type

```
$ hadoop jar Crawler-0.1.jar net.spright.hdfs.HBaseCrawler <URL> <seconds>  /* seconds for shutdown */
```
