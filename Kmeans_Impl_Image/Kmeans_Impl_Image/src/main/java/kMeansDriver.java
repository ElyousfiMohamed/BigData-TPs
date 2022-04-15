import entities.Pixel;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import javax.imageio.ImageIO;
import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;

public class kMeansDriver {
    public static void main(String[] args) throws IOException, URISyntaxException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        boolean done = false;
        int x = 0;

        // create a file that contains the data about the pixels x,y,color(0 - 255)
        /* BufferedImage image = ImageIO.read(kMeansMapper.class.getResource("brain_mri.gif"));
        StringBuffer pixels = new StringBuffer();
        for (int i = 0; i < image.getWidth(); i++) {
            for (int j = 0; j < image.getHeight(); j++) {
                Color mycolor = new Color(image.getRGB(i,j));
                // to ignore the black pixels
                if (mycolor.getRed() > 0)
                    pixels.append(i + " " + j + " " + mycolor.getRed()+"\n");
            }
        }

        FileWriter fileWriter = new FileWriter("pixels.txt");
        PrintWriter printWriter = new PrintWriter(fileWriter);
        printWriter.write(pixels.toString());*/

        while (true) {
            FileSystem fs = FileSystem.get(conf);
            Path f = new Path("./output");
            if (fs.exists(f)) {
                fs.delete(f,true);
            }
            Job job = Job.getInstance(conf, "K_Means " + x++);
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

            FileInputFormat.addInputPath(job, new Path("pixels.txt"));
            FileOutputFormat.setOutputPath(job, new Path("./output"));

            System.out.println("************************************");
            job.waitForCompletion(true);

            f = new Path("./output");
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
                    String[] centers = line.split("\\t");
                    int old = Integer.parseInt(centers[0]);
                    int ne_w = Integer.parseInt(centers[1]);
                    printWriter.println(ne_w);
                    if (old == ne_w)
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