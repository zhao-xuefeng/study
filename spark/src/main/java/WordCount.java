import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;

public class WordCount {
    public static void main(String[] args) {
        SparkContext sparkContext = new SparkContext(new SparkConf().setMaster("local[3]").setMaster("wordCount"));
    }
}
