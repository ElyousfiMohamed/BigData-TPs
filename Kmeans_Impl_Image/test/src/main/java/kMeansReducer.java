import entities.Point;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class kMeansReducer extends Reducer<Text,Text,Text,Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        double[] somme_x_y = new double[2];
        int nb_points = 0;
        Point center = new Point();
        Iterator<Text> it = values.iterator();

        while (it.hasNext()) {
            String[] x_y = it.next().toString().split(" ");
            Point p = new Point(Double.parseDouble(x_y[0]), Double.parseDouble(x_y[1]));
            somme_x_y[0] += p.getX(); somme_x_y[1] += p.getY();
            nb_points++;
        }

        center.setX(somme_x_y[0]/nb_points);
        center.setY(somme_x_y[1]/nb_points);

        context.write(key,new Text(center.toString()));
    }
}
