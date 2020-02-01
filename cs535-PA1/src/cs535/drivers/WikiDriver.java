package cs535.drivers;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class WikiDriver extends Driver {
	
	private void run() {
        SparkConf conf = new SparkConf().setAppName("Budget Analysis");
//        SparkConf conf = new SparkConf().setMaster("local[4]").setAppName("Budget Analysis");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> rdd = sc.textFile(HDFS_MOVIES_METADATA);
	}
	

}
