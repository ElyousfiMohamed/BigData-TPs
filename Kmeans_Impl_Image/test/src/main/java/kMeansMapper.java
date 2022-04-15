import entities.Point;
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

import static entities.Point.convertToPoints;

public class kMeansMapper extends Mapper<LongWritable, Text, Text, Text> {
    List<Integer> centers = new ArrayList<>();

    @Override
    protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException {
        URI uri[] = context.getCacheFiles();
        FileSystem fs = FileSystem.get(context.getConfiguration());
        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(uri[0]))));
        centers.add(Integer.parseInt(br.readLine()));
    }

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        String[] x_y = value.toString().split(",");
        Point p = new Point(Double.parseDouble(x_y[0]), Double.parseDouble(x_y[1])),nearestCenter = null;
        double min = Double.MAX_VALUE, d;
        for (Point c : centers) {
            d = p.distance(c);
            if (d < min) { min = d; nearestCenter = c;}
        }
        System.out.println(nearestCenter + " => " + p.toString());
        context.write(new Text(String.valueOf(nearestCenter)), new Text(p.toString()));
    }
}
