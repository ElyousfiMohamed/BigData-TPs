import org.apache.log4j.spi.RootLogger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.log4j.*;
import scala.Tuple2;

import java.util.Arrays;

public class Spark {
    public static void main(String[] args) {
        RootLogger rootLogger = (RootLogger) Logger.getRootLogger();
        rootLogger.setLevel(Level.ERROR);
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN);
        Logger.getLogger("org.spark-project").setLevel(Level.WARN);

        SparkConf conf = new SparkConf().setAppName("TP7-EXercice1").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> rdd0 = sc.textFile("names");

        JavaRDD<String> rdd1 = sc.parallelize(rdd0.collect());
        System.out.println("rdd1 = " + rdd1.collect());

        JavaRDD<String> rdd2 = rdd1.flatMap(s -> Arrays.asList(s.split(" ")).iterator());
        System.out.println("rdd2 = " + rdd2.collect());

        JavaRDD<String> rdd3 = rdd2.filter(s -> s.length() > 0 && s.length() <= 4);
        System.out.println("rdd3 = " + rdd3.collect());

        JavaRDD<String> rdd4 = rdd2.filter(s -> s.length() > 4 && s.length() <= 8);
        System.out.println("rdd4 = " + rdd4.collect());

        JavaRDD<String> rdd5 = rdd2.filter(s -> s.length() > 8);
        System.out.println("rdd5 = " + rdd5.collect());

        JavaRDD<String> rdd6 = rdd3.union(rdd4);
        System.out.println("rdd6 = " + rdd6.collect());

        JavaRDD<String> rdd71 = rdd5.map(s -> s + "_TP7");
        System.out.println("rdd71 = " + rdd71.collect());

        JavaRDD<String> rdd81 = rdd6.map(s -> s + "_TP7");
        System.out.println("rdd81 = " + rdd81.collect());

        JavaPairRDD<String,Integer> rdd72 = rdd71.mapToPair(s-> new Tuple2<>(s,1));
        System.out.println("rdd72 = " + rdd72.collect());

        JavaPairRDD<String,Integer> rdd82 = rdd81.mapToPair(s-> new Tuple2<>(s,1));
        System.out.println("rdd82 = " + rdd82.collect());

        JavaPairRDD<String,Integer> rdd7=rdd72.reduceByKey((v1, v2) -> v1+v2);
        System.out.println("rdd7 = " + rdd7.collect());

        JavaPairRDD<String,Integer> rdd8=rdd82.reduceByKey((v1, v2) -> v1+v2);
        System.out.println("rdd8 = " + rdd8.collect());

        JavaPairRDD<String,Integer> rdd9=rdd7.union(rdd8);
        System.out.println("rdd9 = " + rdd9.collect());

        JavaPairRDD<String,Integer> rdd10=rdd9.sortByKey();
        System.out.println("rdd10 = " + rdd10.collect());
    }
}
