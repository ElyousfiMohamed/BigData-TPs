package entities;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Point {
    private double x,y;

    public double distance(Point p) { return Math.sqrt(Math.pow((p.getX() - this.getX()), 2) + Math.pow((p.getY() - this.getY()), 2));}

    @Override
    public String toString() {
        return x + " " + y;
    }
    public boolean equals(Point o) { return o.x == x && o.y == y; }

    public static List<Point> convertToPoints(BufferedReader br) throws IOException {
        String ligne;
        List<Point> points = new ArrayList<>();
        while ((ligne = br.readLine()) != null) {
            String[] x_y = ligne.split(" ");
            points.add(new Point(Double.parseDouble(x_y[0]), Double.parseDouble(x_y[1])));
        }
        return points;
    }
}
