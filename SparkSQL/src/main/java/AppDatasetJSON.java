import Model.Employee;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.RootLogger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.max;

public class AppDatasetJSON {
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

        Dataset<Employee> employeeDataset = ss.read().option("multiline","true").option("header",true).json("employees.json")
                .as(Encoders.bean(Employee.class));
        employeeDataset.createOrReplaceTempView("employees");

        // 30 < age < 35
        System.out.println("30 < age < 35");
        employeeDataset.filter(col("age").between(30,35)).show();

        // avg salary
        System.out.println("avg salary");
        employeeDataset.groupBy("departement").avg("salary").show();

        // nombre salariés par departement
        System.out.println("nombre salariés par departement");
        employeeDataset.groupBy("departement").count().show();

        // salaire max
        System.out.println("salaire max");
        employeeDataset.select(max("salary")).show();
    }
}
