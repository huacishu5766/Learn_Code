package SparkCore.D01_instance;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

/**
 * @Auther:huacishu
 * @Date: 2024/6/18
 */
public class Spark02_RDD_Partition {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("Spark");
        JavaSparkContext context = new JavaSparkContext(conf);

        // 创建RDD
        List<String> name = Arrays.asList("shen", "akali", "jie");

        JavaRDD<String> rdd = context.parallelize(name,2);
        //parallelize方法可以传递2个参数，
        //          第一个参数是集合，
        //          第二个参数是分区数
        //          可以不需要制定分区数，spark会采用默认值进行分区 （切片）
        //                  numSlices =  this.scheduler.conf().getInt("spark.default.parallelism", this.totalCores());


    }
}
