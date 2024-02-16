import org.junit.Test;

public class TaskFTest {
    @Test
    public void debug() throws Exception {
        String[] input = new String[4];
        input[0] = "friends.csv";
        input[1] = "access_logs.csv";
        input[2] = "pages.csv";
        input[3] = "output";
        TaskF wc = new TaskF();
        wc.debug(input);
    }
}
