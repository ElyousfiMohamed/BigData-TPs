import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;

public class Application {
    public static void main(String[] args) throws IOException {
        JobConf conf = new JobConf();
        conf.setJobName("Nombre de ventes");
        conf.setJarByClass(Application.class);

        conf.setMapperClass(VentesMapper.class);
        conf.setReducerClass(VentesReducer.class);

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.addInputPath(conf,new Path("ventes.txt"));
        FileOutputFormat.setOutputPath(conf,new Path("./output"));

        JobClient.runJob(conf);
    }
}
