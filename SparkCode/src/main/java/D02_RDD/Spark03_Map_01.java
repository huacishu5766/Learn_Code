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
public class Spark03_Map_01 {
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
        //进行转换

        //传出的数据类型根据业务来定
        // 将 1 2 3 4 转换乘 2 4 6 8
        //JavaRDD<Integer> mapRDD = javaRDD.map(new Function<Integer, Integer>() {
        //    @Override
        //    public Integer call(Integer num) throws Exception {
        //        int i = 0;
        //        i = num * 2;
        //        return i;
        //    }
        //});

        //1、数据类型
        //2、λ表达式
        // TODO 如果Java中接口采用注解@FunctionalInterface声明，那么接口的使用就可以采用JDK提供的函数式编程的语法实现（λ表达式）
        //      1. return 可以省略 : map方法就需要返回值，所以不屑return
        //      2. 分号 可以省略 : 可以采用换行的方式表示代码逻辑
        //      3. 大括号 可以省略 : 如果逻辑代码只有一行
        //      4. 小括号 可以省略 : 参数列表中的参数只有一个
        //      5. 参数和箭头 可以省略 : 参数在逻辑中只使用了一次(需要有对象来实现功能)

        ////1、Java省略
        JavaRDD<Integer> mapRDD1 = javaRDD.map(new Function<Integer, Integer>() {
            @Override
            public Integer call(Integer num) throws Exception {
                return  num * 2;
            }
        });

        // 表达式省略2
        JavaRDD<Integer> mapRDD2 = javaRDD.map(num -> {
                return num * 2;
        });

        //表达式省略3
        JavaRDD<Integer> mapRDD3 = javaRDD.map(num -> num * 2);
        //理解
        // 这个map方法 是对 RDD当中的没一个数据进行转换，每次转换一个，可以简单理解成一个循环，所以
        //每次传入的都是一个参数，传出的也是一个参数，所以采用上述表达式，传入num  传出 num * 2；

        JavaRDD<String> mapRDD4 = javaRDD.map(new Function<Integer, String>() {
            @Override
            public String call(Integer v1) throws Exception {
                return v1 + " ";
            }
        });
        JavaRDD<String> mapRDD5 = javaRDD.map(v1 -> v1 + " ");


        //4、关闭
        context.close();
    }
}
