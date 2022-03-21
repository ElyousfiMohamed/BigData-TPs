import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
public class Application {
        public static void main(String[] args) throws IOException {
                JobConf conf = new JobConf();
                conf.setJobName("Nombre d'employees par departement");
                conf.setJarByClass(Application.class);

                conf.setMapperClass(Employee2Mapper.class);
                conf.setReducerClass(Employee2Reducer.class);

                conf.setOutputKeyClass(Text.class);
                conf.setOutputValueClass(IntWritable.class);

                conf.setInputFormat(TextInputFormat.class);
                conf.setOutputFormat(TextOutputFormat.class);

                FileInputFormat.addInputPath(conf,new Path("employees.csv"));
                FileOutputFormat.setOutputPath(conf,new Path("./output2"));

                JobClient.runJob(conf);
        }

}
