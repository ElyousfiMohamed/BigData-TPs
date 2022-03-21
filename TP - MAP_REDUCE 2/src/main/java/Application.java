import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;

public class Application {
    public static void main(String[] args) throws IOException {
        /*JobConf conf = new JobConf();
        conf.setJobName("Total de ventes");
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
        */

        // ###################################################################"

        JobConf conf2 = new JobConf();
        conf2.setJobName("Total de ventes dans une date");
        conf2.setJarByClass(Application.class);

        conf2.setMapperClass(VentesDateMapper.class);
        conf2.setReducerClass(VentesDateReducer.class);

        conf2.setOutputKeyClass(Text.class);
        conf2.setOutputValueClass(IntWritable.class);

        conf2.setInputFormat(TextInputFormat.class);
        conf2.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.addInputPath(conf2,new Path("ventes.txt"));
        FileOutputFormat.setOutputPath(conf2,new Path("./output2"));

        JobClient.runJob(conf2);
    }
}
