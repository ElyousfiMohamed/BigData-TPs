import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

public class VentesDateMapper extends MapReduceBase
        implements Mapper<LongWritable, Text,Text, IntWritable> {
    @Override
    public void map(LongWritable longWritable, Text text, OutputCollector<Text, IntWritable> outputCollector, Reporter reporter) throws IOException {
        String ventes[] = text.toString().split(" ");
        outputCollector.collect(new Text("\nTotal des ventes sur la ville "+ventes[1]+" dans "+ventes[0] +" ==>"),new IntWritable(Integer.valueOf(ventes[3])));
    }
}
