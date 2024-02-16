import org.junit.Test;

public class TaskCTest {
    @Test
    public void debug() throws Exception {
        String[] input = new String[4];
        input[0] = "pages.csv";
        input[1] = "access_logs.csv";
        input[2] = "friends.csv";
        input[3] = "output";
        TaskC wc = new TaskC();
        wc.debug(input);
    }
}
