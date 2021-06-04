package genrateData;

import org.joda.time.DateTime;

import java.io.File;

public class GenerateTimeStampFile {
    public static void main(String[] args) throws InterruptedException {
        long timestamp=DateTime.now().getMillis();
        for (int i=0;i<50;i++){
            timestamp+=1000l;
            System.out.println("1"+","+timestamp);
        }
    }
}
