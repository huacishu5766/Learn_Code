package SparkCore.D02_RDD;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

/**
 * @Auther:huacishu
 * @Date: 2024/6/28
 */
public class Spark06_GroupBy_02 {
    public static void main(String[] args) throws InterruptedException {
        //1、创建SparkConf对象
        SparkConf conf = new SparkConf();
        //2、加载配置
        conf.setMaster("local[3]");
        conf.setAppName("spark01_env");
        //3、创建SparkContext对象
        JavaSparkContext context = new JavaSparkContext(conf);

        //创建RDD
        List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6);
        JavaRDD<Integer> javaRDD = context.parallelize(list);

        //groupBy
        javaRDD.groupBy(num -> num % 2 == 0 ,2)
                .collect().forEach(System.out::println);

        Thread.sleep(1000000000L);

        //4、关闭
        context.close();
    }
}
