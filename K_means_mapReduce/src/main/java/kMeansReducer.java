import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class kMeansReducer extends Reducer<DoubleWritable,DoubleWritable,DoubleWritable,DoubleWritable> {
    @Override
    protected void reduce(DoubleWritable key, Iterable<DoubleWritable> values, Reducer<DoubleWritable, DoubleWritable, DoubleWritable, DoubleWritable>.Context context) throws IOException, InterruptedException {
        double somme= 0;
        int nb_points = 0;
        Iterator<DoubleWritable> it = values.iterator();

        while (it.hasNext()) {
            somme += it.next().get();
            nb_points++;
        }

        double mean = somme/nb_points;
        context.write(key,new DoubleWritable(mean));
    }
}
