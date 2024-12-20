package SparkCore.D02_RDD;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @Auther:huacishu
 * @Date: 2024/6/22
 */

/**
 * 从内存当中加载数据进RDD，从集合当中创建RDD
 */
public class Spark02_MemoryRDD {
    public static void main(String[] args) {
        //创建Spark环境。
        //1、创建SparkConf对象
        SparkConf conf = new SparkConf();
        //2、加载配置
        conf.setMaster("local[2]");
        conf.setAppName("spark01_env");
        //3、创建SparkContext对象
        JavaSparkContext context = new JavaSparkContext(conf);

        //1、加载数据
        ArrayList<Integer> list = new ArrayList<>();
        list.add(1);
        list.add(2);
        list.add(3);
        list.add(4);
        System.out.println(list);

        List<String> s1 = Arrays.asList("zhangsan", "lisi", "wangwu");

        //2、将数据加载进spark 加载进RDD，创建RDD
        JavaRDD<String> javaRDD = context.parallelize(s1);
        //3.处理数据
        //4.保存结果
        javaRDD.saveAsTextFile("output");


        //4、关闭context
        context.stop();
    }
}
