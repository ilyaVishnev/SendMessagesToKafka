package ru;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.streaming.Minutes;
import org.apache.spark.streaming.Seconds;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.io.IOException;
import java.net.InetAddress;
import java.sql.*;
import java.util.*;

public class MyAppllication {

    public static void main(String... args) throws ClassNotFoundException, SQLException, InterruptedException {
        SparkConf conf = new SparkConf().setAppName("MyAppllication").setMaster("local");
        JavaSparkContext jc = new JavaSparkContext(conf);
        MyKafkaProducer myKafkaProducer = new MyKafkaProducer();
        JavaStreamingContext scc = new JavaStreamingContext(jc, Minutes.apply(5));
        JavaReceiverInputDStream DStream = scc.receiverStream(new ReceiverFromDiv());
        JavaStreamingContext sccTwo = new JavaStreamingContext(jc, Minutes.apply(20));
        JavaReceiverInputDStream DStreamTwo = scc.receiverStream(new ReceiverFromDataBase());
        if (ReceiverFromDiv.getSize() > ReceiverFromDataBase.getMax() && ReceiverFromDiv.getSize() < ReceiverFromDataBase.getMin()) {
            MyKafkaProducer.sendToKafka();
        }
        scc.start();
        scc.awaitTermination();
        sccTwo.start();
        sccTwo.awaitTermination();
    }

}
