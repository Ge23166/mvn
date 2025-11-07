package count;

import junit.framework.TestCase;

public class SortDriverTest extends TestCase {
    public int run(String[] args) throws Exception {
        SortDriver driver = new SortDriver();
        driver.run(args);
        return 0;
    }

}