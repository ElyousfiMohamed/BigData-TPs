import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import javax.ws.rs.core.Application;
import java.io.IOException;

public class Applciation {
    public static void main(String[] args) throws IOException {
        JobConf conf = new JobConf();
        conf.setJobName("Nombre de mots");
        conf.setJarByClass(Application.class);

        conf.setMapperClass(OccurencesMapper.class);
        conf.setReducerClass(OccurencesReduce.class);

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.addInputPath(conf,new Path("prenom.txt"));
        FileOutputFormat.setOutputPath(conf,new Path("./output"));

        JobClient.runJob(conf);
    }
}
