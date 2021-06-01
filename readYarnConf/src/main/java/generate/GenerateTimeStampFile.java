package generate;

import org.joda.time.DateTime;

import java.io.File;

public class GenerateTimeStampFile {
    public static void main(String[] args) throws InterruptedException {
        for (int i=0;i<50;i++){
            System.out.println("1"+","+DateTime.now().getMillis());
            Thread.sleep(2000l);
        }
    }
}
