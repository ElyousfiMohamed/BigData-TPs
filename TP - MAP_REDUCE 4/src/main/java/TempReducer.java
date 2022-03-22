import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Iterator;

public class TempReducer extends MapReduceBase
        implements Reducer<Text, IntWritable,Text, IntWritable> {

    @Override
    public void reduce(Text text, Iterator<IntWritable> iterator, OutputCollector<Text, IntWritable> outputCollector, Reporter reporter) throws IOException {
        int min = 0;
        int temp;
        int i = 0;
        while (iterator.hasNext()){
            if(i==0) {
                min= iterator.next().get();
                i++;
            }
            temp = iterator.next().get();
            if(min > temp)
                min = temp;
        }
        outputCollector.collect(text,new IntWritable(min));
    }
}
