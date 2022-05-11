import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.RootLogger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

public class Application {
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

        Dataset<Row> df = ss.read().option("multiline","true").json("employees.json");
        // 30 < age < 35
        df.filter(col("age").between(30,35)).show();

        // avg salary
        df.groupBy("departement").avg("salary").show();

        // nombre salariés par departement
        df.groupBy("departement").count().show();

        // salaire max
        df.select(max("salary")).show();
    }
}
