import entities.Pixel;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

public class kMeansMapper extends Mapper<LongWritable, Text, Text, Text> {
    List<Integer> centers = new ArrayList<>();

    @Override
    protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException {
        URI uri[] = context.getCacheFiles();
        FileSystem fs = FileSystem.get(context.getConfiguration());
        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(uri[0]))));
        String ligne = "";
        while ((ligne = br.readLine()) != null) {
            centers.add(Integer.parseInt(ligne));
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        String[] x_y_c = value.toString().split(" ");
        Pixel p = new Pixel(Double.parseDouble(x_y_c[0]), Double.parseDouble(x_y_c[1]), Integer.parseInt(x_y_c[2]));
        int min = Integer.MAX_VALUE, d, nearestCenter = 0;
        for (Integer c : centers) {
            d = Math.abs(p.getColor() - c.intValue());
            if (d < min) {
                min = d;
                nearestCenter = c;
            }
        }
        context.write(new Text(String.valueOf(nearestCenter)), new Text(p.toString()));
    }
}
