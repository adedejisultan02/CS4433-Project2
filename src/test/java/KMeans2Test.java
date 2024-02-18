import org.junit.Test;

public class KMeans2Test {
    @Test
    public void testNationalityCount() throws Exception {
        String[] input = new String[4];
        input[0] = "dataset.csv";
        input[1] = "seeds.csv";
        input[2] = "output";
        input[3] = String.valueOf(3);

        KMeansWrapper wc = new KMeansWrapper();
        wc.debug(input);

    }
}
