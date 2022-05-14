import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.RootLogger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

import static org.apache.spark.sql.functions.*;

public class AppDataframeCSV {
    public static void main(String[] args) {
        RootLogger rootLogger = (RootLogger) Logger.getRootLogger();
        rootLogger.setLevel(Level.ERROR);
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN);
        Logger.getLogger("org.spark-project").setLevel(Level.WARN);

        SparkSession ss = SparkSession.builder().
                appName("Spark SQL").
                master("local[*]").
                config("spark.some.config.option", "some-value").
                getOrCreate();

        Dataset<Row> df = ss.read().option("multiline","true").option("header",true).csv("employees.csv");
        df.createOrReplaceTempView("employees");

        // 30 < age < 35
        System.out.println("30 < age < 35");
        df.filter(col("age").between(30,35)).show();

        // avg salary
        System.out.println("avg salary");
        ss.sql("select cast(avg(salary) as double) from employees group by departement;").show();

        // nombre salariés par departement
        System.out.println("nombre salariés par departement");
        df.groupBy("departement").count().show();

        // salaire max
        System.out.println("salaire max");
        ss.sql("select max(cast(salary  as double)) from employees;").show();
    }
}
