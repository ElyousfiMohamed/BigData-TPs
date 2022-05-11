import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.RootLogger;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;

public class SparkStreaming1 {
    public static void main(String[] args) throws InterruptedException {
        RootLogger rootLogger = (RootLogger) Logger.getRootLogger();
        rootLogger.setLevel(Level.ERROR);
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN);
        Logger.getLogger("org.spark-project").setLevel(Level.WARN);

        SparkConf sparkConf = new SparkConf().setAppName("SparkStreaming1").setMaster("local[*]");
        JavaStreamingContext jsc = new JavaStreamingContext(sparkConf, Durations.seconds(30));
        JavaReceiverInputDStream<String> lignes = jsc.socketTextStream("localhost",9090);

        JavaDStream<String> mots = lignes.flatMap(s -> Arrays.asList(s.split(" ")).iterator());
        JavaPairDStream<String,Integer> map = mots.mapToPair(s -> new Tuple2<>(s,1));
        JavaPairDStream<String,Integer> reduce = map.reduceByKey((v1,v2) -> v1 + v2);

        reduce.print();
        jsc.start();
        jsc.awaitTermination();
    }
}
