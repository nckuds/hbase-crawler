package net.spright.hdfs;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.jsoup.HttpStatusException;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseCrawler {
    
    private static List putList;
    public static void main(String[] args) throws InterruptedException, IOException {
        final int downloadThreadCount = 10;
        final int htmlThreadCount = 10;
        final int flushThreadCount = 10;
        final int urlCount = 10000;
        final int htmlCount = 1000;
        final int resultCount = 1000;
        final Configuration configuration = HBaseConfiguration.create();
        putList = Collections.synchronizedList(new LinkedList());

        ExecutorService service = Executors.newFixedThreadPool(
            downloadThreadCount +
            htmlThreadCount +
            flushThreadCount
            );
        BlockingQueue<String> urlQueue = new ArrayBlockingQueue(urlCount);
        BlockingQueue<Document> htmlQueue = new ArrayBlockingQueue(htmlCount);
        BlockingQueue<HtmlResult> resultQueue = new ArrayBlockingQueue(resultCount);
        urlQueue.put(args[0]);

        for (int i = 0; i != downloadThreadCount; ++i) {
            service.execute(new HtmlDownloader(urlQueue, htmlQueue));
        }
        for (int i = 0; i != htmlThreadCount; ++i) {
            service.execute(new HtmlParser(urlQueue, htmlQueue, resultQueue));
        }
        for (int i = 0; i != flushThreadCount; ++i) {
            service.execute(new HtmlResultFlusher(configuration, resultQueue));
        }
        service.shutdown();
        //waitSomething(Integer.parseInt(args[1]));
        //service.shutdownNow();
        service.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
        System.exit(0);
    }
    private static void waitSomething(int time) throws InterruptedException {
        TimeUnit.SECONDS.sleep(time);
    }
    private static class HtmlDownloader implements Runnable {
        private final BlockingQueue<String> urlQueue;
        private final BlockingQueue<Document> htmlQueue;
        private static ArrayList<String> urlList =  new ArrayList<String>();
        
        public HtmlDownloader(
            BlockingQueue<String> urlQueue,
            BlockingQueue<Document> htmlQueue
            ) {
            this.urlQueue = urlQueue;
            this.htmlQueue = htmlQueue;
        }
        @Override
        public void run() {
            try {
                while (!Thread.interrupted()) {
                    //TODO
                    while (urlQueue.isEmpty()) {
                        if (urlQueue.isEmpty() && htmlQueue.isEmpty()){
                            break;
                        }
                       
                    } // waiting
                    
                    
                    String url = urlQueue.take();  
                    if (urlList.size() > 1000000) {
                        urlList.clear();
                    }
                    //System.out.println(urlQueue.size());
                    if (!urlList.contains(url)) {
                        try {      
                            urlList.add(url);
                            Document doc = null;  
                            Elements meta  = null;
                            try {
                                doc = Jsoup.connect(url).get();
                            } catch (IOException  ex) {
                                //Logger.getLogger(MutiThreadCrawler.class.getName()).log(Level.SEVERE, null, ex);
                            }
                           
                            if (doc == null) {
                                continue;
                            }
                            meta = doc.select("html head meta");
                            
                            if (meta != null && meta.attr("http-equiv").contains("REFRESH")) {
                                try {
                                    doc = Jsoup.connect(meta.attr("content").split("=")[1]).get();
                                } catch (IOException ex) {
                                    //Logger.getLogger(MutiThreadCrawler.class.getName()).log(Level.SEVERE, null, ex);
                                }
                            }
                            
                            if (doc != null && htmlQueue.size() < 999) {
                                htmlQueue.put(doc);
                            }
                            //System.out.println(doc.title());
                        }
                        catch (IllegalArgumentException e) {
                          //System.out.println("url is not vaild");
                          //System.out.println(url);
                        } 
                    }
                }
            } catch (InterruptedException e) {
                //System.out.println("urlQueue interrupted");
                Thread.currentThread().interrupt(); 
            }
        }
    }
    private static class HtmlParser implements Runnable {
        private final BlockingQueue<String> urlQueue;
        private final BlockingQueue<Document> htmlQueue;
        private final BlockingQueue<HtmlResult> resultQueue;
        public HtmlParser(BlockingQueue<String> urlQueue, BlockingQueue<Document> htmlQueue, BlockingQueue<HtmlResult> resultQueue) {
            this.urlQueue = urlQueue;
            this.htmlQueue = htmlQueue;
            this.resultQueue = resultQueue;
        }
        @Override
        public void run() {
            try {
                while (!Thread.interrupted()) {
                    
                    while (htmlQueue.isEmpty()) {
                        if (urlQueue.isEmpty() && htmlQueue.isEmpty()) {
                            break;
                        }
                        //System.out.println("html empty");
                    } // waiting
                    
                    
                    Document doc = htmlQueue.take();
                    //System.out.println(doc.title());
                    if (!doc.title().isEmpty()) {
                     
                        HtmlResult htmlResult = new HtmlResult(doc.location(), doc.title(), doc.outerHtml());
                        Elements links = doc.select("a[href]");
                        
                        if (resultQueue.size() < 999) {
                            //System.out.println(htmlResult.title);
                            resultQueue.put(htmlResult);
                        }
                        
                        for(Element link : links) {    
                            if (urlQueue.size() < 9999) {
                                urlQueue.put(link.attr("abs:href"));
                            }

                        }
                    }
                }
            } catch (InterruptedException ex) {           
                Thread.currentThread().interrupt(); 
            }
        }
    }
    
    private static class HtmlResultFlusher implements Runnable {
       
        private final BlockingQueue<HtmlResult> resultQueue;
        private final Configuration conf;
        DateFormat dateFormat = new SimpleDateFormat("yyyy_MM_dd HH-mm-ss");
        
        public HtmlResultFlusher(
                Configuration conf,
                BlockingQueue<HtmlResult> resultQueue
            ) {
            this.resultQueue = resultQueue;
            this.conf = conf;
        }
        @Override
        public void run() {

            try {
                while (!Thread.interrupted()) {
                    //System.out.println(resultQueue.size());
                    //while (resultQueue.isEmpty()) {
                       //  System.out.println("empty");
                    //} // waiting
                    if (resultQueue.isEmpty()) {
                        continue;
                    }
                    Date date = new Date();
                    HtmlResult htmlResult = resultQueue.take();
                    System.out.println("doc take");
                    if (htmlResult.title != null) {
                    
                        System.out.println("title: " + htmlResult.title);
                        System.out.println("link: " + htmlResult.link + "\n");
                        outputHtml(conf, htmlResult,  "page_" + htmlResult.title 
                            + dateFormat.format(date));
                    }
                    
                }
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt(); 
            } catch (IOException ex) {
                Logger.getLogger(HBaseCrawler.class.getName()).log(Level.SEVERE, null, ex);
            } 
        }
    }

    public static void outputHtml(Configuration conf, HtmlResult htmlResult,
        String outputName) throws IOException  {
        try(Connection connection = ConnectionFactory.createConnection(conf)) {
            try(Table table = connection.getTable(TableName.valueOf("page"))) {
                Put put = new Put( Bytes.toBytes(outputName) );
                put.add(Bytes.toBytes("title"), Bytes.toBytes(""), Bytes.toBytes(htmlResult.getTitle()));
                put.add(Bytes.toBytes("link"), Bytes.toBytes(""), Bytes.toBytes(htmlResult.getLink()));
                put.add(Bytes.toBytes("content"), Bytes.toBytes(""), Bytes.toBytes(htmlResult.getContent()));
                putList.add(put);
            }
        }
    }
    
 
    private static class HtmlResult {
       private final String link;
       private final String title;
       private final String content;
       public HtmlResult(
           String link,
           String title,
           String content
           ) {
           this.link = link;
           this.title = title;
           this.content = content;
       }
       public String getTitle() {
           return title;
       }
       public String getLink() {
           return link;
       }
       public String getContent() {
           return content;
       }
   }

}
