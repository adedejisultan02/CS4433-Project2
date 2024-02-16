import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class dataGen {

    public static void main(String[] args) throws Exception {
        // Change to 3000
        int dataSize = 3000;
        int maxValueXY = 5000;

        writeDatasetToCSV(dataSize, maxValueXY, "dataset.csv", false);
    }

    public static void writeDatasetToCSV(int size, int maxValue, String filename, boolean centroids) {
        try (PrintWriter writer = new PrintWriter(new FileWriter(filename))) {
            Random random = new Random();

            if (centroids) {
                // Read from existing dataset file
                List<String> points = getListFromFile("dataset.csv");
                for (int i = 0; i < size; i++) {
                    int index = random.nextInt(points.size());
                    writer.println(points.get(index));
                }
            } else {
                // Write data
                for (int i = 0; i < size; i++) {
                    int x = random.nextInt(maxValue + 1);
                    int y = random.nextInt(maxValue + 1);
                    writer.println(x + "," + y);
                }
            }
        } catch (IOException e) {
            System.out.println("CSV file failed.");
        }
    }

    public static List<String> getListFromFile(String fileName) {
        List<String> output = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(fileName))) {
            String line;
            while ((line = reader.readLine()) != null) {
                output.add(line);
            }
        } catch (FileNotFoundException e) {
            System.out.println("File not found");
        } catch (IOException e) {
            System.out.println(e);
        }
        return output;
    }
}
