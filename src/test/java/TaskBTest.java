import org.junit.Test;

public class TaskBTest {
    @Test
    public void debug() throws Exception {
        String[] input = new String[2];
        input[0] = "access_logs.csv";
        input[1] = "output";
        TaskB wc = new TaskB();
        wc.debug(input);
    }
}
