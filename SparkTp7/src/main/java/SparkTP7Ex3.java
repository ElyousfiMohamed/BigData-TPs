import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.RootLogger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.File;

public class SparkTP7Ex3 {
    public static void main(String[] args) {
        RootLogger rootLogger = (RootLogger) Logger.getRootLogger();
        rootLogger.setLevel(Level.ERROR);
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN);
        Logger.getLogger("org.spark-project").setLevel(Level.WARN);

        SparkConf conf = new SparkConf().setAppName("TP7-EXercice3").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        File dir = new File("files");
        File[] liste = dir.listFiles();

        for (File f : liste) {
            System.out.println("\n********************************************");
            System.out.println(f.getName());
            System.out.println("********************************************");
            JavaRDD<String> rdd0 = sc.textFile(f.getPath());
            JavaRDD<String> rddTMin = rdd0.filter(s -> s.contains("TMIN"));
            JavaRDD<Integer> rddMin = rddTMin.map(s -> Integer.parseInt(s.split(",")[3]));
            Double mean = Double.valueOf(rddMin.reduce((v1, v2) -> v1 + v2));
            long count = rddMin.count();
            System.out.println("Temperature minimale moyenne : " + mean / count);

            JavaRDD<String> rddTMax = rdd0.filter(s -> s.contains("TMAX"));
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
        }
    }
}