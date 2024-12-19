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
public class Spark06_GroupBy_01 {
    public static void main(String[] args) {
        //1、创建SparkConf对象
        SparkConf conf = new SparkConf();
        //2、加载配置
        conf.setMaster("local[*]");
        conf.setAppName("spark01_env");
        //3、创建SparkContext对象
        JavaSparkContext context = new JavaSparkContext(conf);

        //创建RDD
        List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6);
        JavaRDD<Integer> javaRDD = context.parallelize(list);

        //groupBy
        // // TODO RDD的方法：groupBy,按照指定的规则对数据进行分组
        javaRDD.groupBy(new Function<Integer, Object>() {

            @Override
            public Object call(Integer v1) throws Exception {

                // 返回的值其实就是数据对应的组的名称,相同组的名称的数据会放置在一个组中
                // 当前的逻辑就是给数据增加标记
                // 1 -> B
                // 2 -> B
                // 3 -> B
                // 4 -> B
                return "B";
            }
        });

        //将数据按照奇偶数 分组
        javaRDD.groupBy(new Function<Integer, Object>() {
            @Override
            public Object call(Integer num) throws Exception {
                if (num % 2 == 0 ){
                    return true;
                }else{
                    return false;
                }
            }
        });

        //lambda 表达式

        javaRDD.groupBy(num -> {
            if (num % 2 == 0 ){
                return true;
            }else{
                return false;
            }
        });

        javaRDD.groupBy(num -> num % 2 == 0 )
                .collect().forEach(System.out::println);



        //4、关闭
        context.close();
    }
}
