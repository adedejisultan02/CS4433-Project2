import org.junit.Test;
import static org.junit.Assert.*;

public class Project1Test {
    @Test
    public void debug() throws Exception {
        String[] input = new String[2];
        input[0] = "data.txt";
        input[1] = "output";
        Project1 wc = new Project1();
        wc.debug(input);
    }
}