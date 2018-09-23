package rizhi;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogGenerator {

    private static Logger logger= LoggerFactory.getLogger(LogGenerator.class);
    public static void main(String[] args) throws InterruptedException {

        int index=0;
        while(true)
        {
            Thread.sleep(1000);
            logger.info("value:"+index++);
        }


    }
}
