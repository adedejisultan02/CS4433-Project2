import org.junit.Test;

public class AdvancedKMeansTest {
    @Test
    public void testNationalityCount() throws Exception {
        String[] input = new String[4];
        input[0] = "dataset.csv";
        input[1] = "seeds.csv";
        input[2] = "output";
        input[3] = String.valueOf(9);

        AdvancedKMeans wc = new AdvancedKMeans();
        wc.debug(input);

    }
}
