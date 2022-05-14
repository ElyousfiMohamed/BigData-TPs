import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.RootLogger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;

public class AppDataframeCSVSql {
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
        System.out.println("30 < age < 35 - SQL VERSION");
        ss.sql("select * from employees where age between 30 and 35;").show();

        // avg salary
        System.out.println("avg salary - SQL VERSION");
        ss.sql("select cast(avg(salary) as double) from employees group by departement;").show();

        // nombre salariés par departement
        System.out.println("nombre salariés par departement - SQL VERSION");
        ss.sql("select departement,count(*) from employees group by departement").show();

        // salaire max
        System.out.println("salaire max - SQL VERSION");
        ss.sql("select max(cast(salary  as double)) from employees;").show();
    }
}
