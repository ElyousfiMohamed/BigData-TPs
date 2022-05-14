import Model.Employee;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.RootLogger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class AppDatasetJSONSql {
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
        System.out.println("30 < age < 35 - SQL VERSION");
        ss.sql("select * from employees where age between 30 and 35").show();

        // avg salary
        System.out.println("avg salary - SQL VERSION");
        ss.sql("select departement,avg(salary) from employees group by departement").show();

        // nombre salariés par departement
        System.out.println("nombre salariés par departement - SQL VERSION");
        ss.sql("select departement,count(*) from employees group by departement").show();

        // salaire max
        System.out.println("salaire max - SQL VERSION");
        ss.sql("select max(salary) from employees").show();
    }
}
