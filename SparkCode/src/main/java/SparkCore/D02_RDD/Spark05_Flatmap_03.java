package SparkCore.D02_RDD;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * @Auther:huacishu
 * @Date: 2024/6/28
 */
public class Spark05_Flatmap_03 {
    public static void main(String[] args) {
        //1、创建SparkConf对象
        SparkConf conf = new SparkConf();
        //2、加载配置
        conf.setMaster("local[*]");
        conf.setAppName("spark01_env");
        //3、创建SparkContext对象
        JavaSparkContext context = new JavaSparkContext(conf);

        //创建RDD
        JavaRDD<String> javaRDD = context.textFile("input/spark.text");




        //map
        JavaRDD<String[]> mapRDD = javaRDD.map(new Function<String, String[]>() {
            @Override
            public String[] call(String line) throws Exception {
                String[] s = line.split(" ");
                return s;
            }
        });
        //map的结果
        //[hadoop,spark,java,hdfs]
        //[hadoop,spark,java]
        //[hadoop,spark]

        //map的lambda表达式
        javaRDD.map(line -> line.split(" "));


        //flatmap
        JavaRDD<String> flatMapRDD = javaRDD.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String line) throws Exception {
                String[] words = line.split(" ");

                List<String> list = Arrays.asList(words);
                return list.iterator();
            }
        });
        //flatmap的结果
        // hadoop
        //spark

        //flatmap lambda 表达式
        javaRDD.flatMap(line -> {
            String[] words = line.split(" ");

            List<String> list = Arrays.asList(words);
            return list.iterator();
        });

        javaRDD.flatMap(line -> Arrays.asList(line.split(" ")).iterator());

        flatMapRDD.collect().forEach(System.out::println);

        //最终形式
        //JavaRDD<String> javaRDD = context.textFile("input/spark.text");

        javaRDD.flatMap(line -> Arrays.asList(line.split(" ")).iterator())
                .collect()
                .forEach(System.out::println);


        //4、关闭
        context.close();
    }
}
