import Clusters.Cluster;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class Application {
    static final int NB_POINTS = 100;
    static final int NB_CLUSTERS = 3;

    public static void main(String[] args) {
        List<Double> points = new ArrayList<>();
        List<Cluster> clusters = new ArrayList<>();

        for (int i = 0; i < NB_POINTS; i++) {
            points.add(Math.random() * 100);
        }

        for (int i = 0; i < NB_CLUSTERS; i++) {
            Cluster cluster = new Cluster();
            cluster.setCenter(Math.random() * 100);
            clusters.add(cluster);
        }

        boolean flag = false;

        while (!flag) {
            List<Double> center = new ArrayList<>();

            for (Cluster c: clusters) {
                center.add(c.getCenter());
            }

            Cluster temp = new Cluster();
            for (double d : points) {
                double min = Math.abs(clusters.get(0).getCenter() - d);
                for (Cluster c : clusters) {
                    if (min >= Math.abs(c.getCenter() - d)) {
                        min = Math.abs(c.getCenter() - d);
                        temp = c;
                    }
                }
                temp.getPoints().add(d);
            }

            for (Cluster c : clusters) {
                double mean, somme = 0;
                for (int i = 0; i < c.getPoints().size(); i++) {
                    somme += c.getPoints().get(i);
                }
                c.setCenter(somme / c.getPoints().size());

            }

            for (int i = 0; i < NB_CLUSTERS; i++) {
                if(clusters.get(i).getCenter() == center.get(i))
                    flag = true;
                else {
                    flag = false;
                    break;
                }
            }

            for (Cluster c : clusters) {
                System.out.println(c.getCenter()+" : "+c.getPoints());
            }

            for (Cluster c: clusters) {
                c.getPoints().clear();
            }
        }
    }
}
