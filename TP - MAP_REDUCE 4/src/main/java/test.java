import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;

public class test {
    public static void main(String[] args) throws FileNotFoundException {
        File getCSVFiles = new File("temp.csv");
        Scanner sc = new Scanner(getCSVFiles);
        sc.useDelimiter(",");
        while (sc.hasNext())
        {
            System.out.print(sc.next() + " | ");
        }
        sc.close();
    }
}
