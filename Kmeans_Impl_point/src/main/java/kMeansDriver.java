import entities.Point;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;

public class kMeansDriver {
    public static void main(String[] args) throws IOException, URISyntaxException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        boolean done = false;
        int i = 0;

        while (true) {
            Job job = Job.getInstance(conf, "K_Means " + i++);
            job.setJarByClass(kMeansDriver.class);

            job.setMapperClass(kMeansMapper.class);
            job.setReducerClass(kMeansReducer.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);

            job.addCacheFile(new URI("center.txt"));

            FileInputFormat.addInputPath(job, new Path("points.csv"));
            FileOutputFormat.setOutputPath(job, new Path("./output"));

            System.out.println("************************************");
            job.waitForCompletion(true);

            FileSystem fs = FileSystem.get(conf);
            Path f = new Path("./output");
            if (fs.exists(f)) {
                InputStreamReader is = new InputStreamReader(fs.open(new Path("./output/part-r-00000")));
                BufferedReader br = new BufferedReader(is);

                FileWriter fileWriter = new FileWriter("center.txt", false);
                PrintWriter printWriter = new PrintWriter(fileWriter);
                printWriter.write("");

                fileWriter = new FileWriter("center.txt", true);
                printWriter = new PrintWriter(fileWriter);

                String line = "";
                while ((line = br.readLine()) != null) {
                    String[] parts = line.split("\\t");
                    Point p0 = new Point(Double.parseDouble(parts[0].split(" ")[0]), Double.parseDouble(parts[0].split(" ")[1]));
                    Point p = new Point(Double.parseDouble(parts[1].split(" ")[0]), Double.parseDouble(parts[1].split(" ")[1]));
                    printWriter.println(p);
                    if (p0.equals(p))
                        done = true;
                }

                if (done)
                    break;

                printWriter.close();
                fs.delete(f, true);
            }
        }
    }
}
