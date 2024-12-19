package D02_RDD;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.util.Arrays;
import java.util.List;

/**
 * @Auther:huacishu
 * @Date: 2024/6/28
 */
public class Spark04_Filter_01 {
    public static void main(String[] args) {
        //1、创建SparkConf对象
        SparkConf conf = new SparkConf();
        //2、加载配置
        conf.setMaster("local[*]");
        conf.setAppName("spark01_env");
        //3、创建SparkContext对象
        JavaSparkContext context = new JavaSparkContext(conf);

        //创建RDD
        List<Integer> list = Arrays.asList(1, 2, 3, 4);
        JavaRDD<Integer> javaRDD = context.parallelize(list);

        //TODO Filter 过滤的意思
        //  RDD可以根据指定的过滤规则对数据源中的数据进行筛选过滤
        //        如果满足规则（返回结果true），那么数据保留，如果不满足规则（返回结果false），那么数据就会丢弃

        //定义规则： 只保留偶数
        JavaRDD<Integer> filterRDD = javaRDD.filter(new Function<Integer, Boolean>() {
            @Override
            public Boolean call(Integer num) throws Exception {
                if (num % 2 == 0) {
                    return true;
                } else
                    return false;
            }
        });

        //lambda 表达式
        JavaRDD<Integer> filterRDD1 = javaRDD.filter(num -> {
            if (num % 2 == 0) {
                return true;
            } else {
                return false;
            }
        });

        javaRDD.filter(num -> {
            return num % 2 == 0 ;
        });

        javaRDD.filter(num -> num % 2 == 0);


        //打印
        filterRDD.collect().forEach(System.out::println);

        //4、关闭
        context.close();
    }
}
