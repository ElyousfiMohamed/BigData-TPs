import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.RootLogger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Int;
import scala.Tuple2;

import java.util.Arrays;

public class SparkStreaming3 {
    public static Integer toInteger(JavaDStream<Integer> j) {

    }

    public static void main(String[] args) throws InterruptedException {
        RootLogger rootLogger = (RootLogger) Logger.getRootLogger();
        rootLogger.setLevel(Level.ERROR);
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN);
        Logger.getLogger("org.spark-project").setLevel(Level.WARN);

        SparkConf sparkConf = new SparkConf().setAppName("SparkStreaming1").setMaster("local[*]");
        JavaStreamingContext jsc = new JavaStreamingContext(sparkConf, Durations.seconds(30));
        JavaDStream<String> lignes = jsc.textFileStream("hdfs://localhost:9000/files/");

        JavaDStream<String> rddTMin = lignes.filter(s -> s.contains("TMIN"));
        JavaDStream<Integer> rddMin = rddTMin.map(s -> Integer.parseInt(s.split(",")[3]));
        JavaDStream<Integer> total = rddMin.reduce((v1, v2) -> v1 + v2);
        JavaDStream<Long> count = rddMin.count();
        Integer i = toInteger(total);


        /*JavaRDD<String> rddTMax = rdd0.filter(s -> s.contains("TMAX"));
        JavaRDD<Integer> rddMax = rddTMax.map(s -> Integer.parseInt(s.split(",")[3]));
        mean = Double.valueOf(rddMax.reduce((v1, v2) -> v1 + v2));
        count = rddMax.count();
        System.out.println("Temperature maximale moyenne : " + mean / count);

        int max = rddMax.reduce((v1, v2) -> v1 > v2 ? v1 : v2);
        int min = rddMin.reduce((v1, v2) -> v1 < v2 ? v1 : v2);

        System.out.println("Temperature maximale la plus élevée : " + max);
        System.out.println("Temperature minimale la plus basse : " + min);

        JavaPairRDD<Integer, String> rdd5Chaudes = rddTMax.mapToPair(s -> new Tuple2<>(
                Integer.parseInt(s.split(",")[3]), s.split(",")[0])
        );
        System.out.println("Top 5 des stations meteo les plus chaudes : " + rdd5Chaudes.sortByKey(false).take(5));

        JavaPairRDD<Integer, String> rdd5Froides = rddTMin.mapToPair(s -> new Tuple2<>(
                Integer.parseInt(s.split(",")[3]), s.split(",")[0])
        );
        System.out.println("Top 5 des stations meteo les plus froides. : " + rdd5Froides.sortByKey(true).take(5));
*/
        total.print();
        jsc.start();
        jsc.awaitTermination();
    }
}
