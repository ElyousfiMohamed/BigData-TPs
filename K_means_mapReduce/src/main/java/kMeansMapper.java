import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

public class kMeansMapper extends Mapper<LongWritable, Text, DoubleWritable,DoubleWritable> {
    List<Double> center = new ArrayList<>();

    @Override
    protected void setup(Mapper<LongWritable, Text, DoubleWritable, DoubleWritable>.Context context) throws IOException, InterruptedException {
        URI uri[] = context.getCacheFiles();
        FileSystem fs = FileSystem.get(context.getConfiguration());
        InputStreamReader is = new InputStreamReader(fs.open(new Path(uri[0])));
        BufferedReader br = new BufferedReader(is);
        String ligne = null;
        while((ligne = br.readLine())!=null) {
            center.add(Double.parseDouble(ligne));
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, DoubleWritable, DoubleWritable>.Context context) throws IOException, InterruptedException {
        double p = Double.parseDouble(value.toString());
        double min = Double.MAX_VALUE,d,nearestCenter = 0;

        for (double c: center) {
            d = Math.abs(p-c);
            if(d < min) {
               min = d;
               nearestCenter = c;
            }
        }

        context.write(new DoubleWritable(nearestCenter), new DoubleWritable(p));
    }
}
