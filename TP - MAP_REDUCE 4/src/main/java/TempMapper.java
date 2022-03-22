import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;

public class TempMapper extends MapReduceBase
        implements Mapper<LongWritable, Text,Text, IntWritable> {
    @Override
    public void map(LongWritable longWritable, @NotNull Text text, OutputCollector<Text, IntWritable> outputCollector, Reporter reporter) throws IOException {
        String temps[] = text.toString().split("\",\"");
        StringBuilder temp = new StringBuilder(temps[13]);
        char sign = temp.charAt(0);
        temp = temp.deleteCharAt(0);
        temp = new StringBuilder((temp.toString().split(",")[0]+"."+temp.toString().split(",")[1]));
        float temper = Float.parseFloat(temp.toString());
        if(sign == '-')
            temper*=-1;

        outputCollector.collect(new Text("Mois "+ temps[1].split("-")[1] + " | Temperature minimum : "),new IntWritable((int)temper));
    }
}
