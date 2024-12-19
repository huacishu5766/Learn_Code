package D02_RDD;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * @Auther:huacishu
 * @Date: 2024/6/28
 */
public class Spark05_Flatmap_02 {
    public static void main(String[] args) {
        //1、创建SparkConf对象
        SparkConf conf = new SparkConf();
        //2、加载配置
        conf.setMaster("local[*]");
        conf.setAppName("spark01_env");
        //3、创建SparkContext对象
        JavaSparkContext context = new JavaSparkContext(conf);

        List<Integer> list = Arrays.asList(1, 2, 3, 4);
        //创建RDD
        List<List<Integer>> lists = Arrays.asList(
                Arrays.asList(1, 2),
                Arrays.asList(3, 4)
        );
        JavaRDD<List<Integer>> javaRDD = context.parallelize(lists);

        // TODO RDD的转换方法：flatmap (扁平映射)
        //      flat(数据扁平化) + map(映射)

        // 1234 转换 2468
        JavaRDD<Integer> flatMapJavaRDD = javaRDD.flatMap(new FlatMapFunction<List<Integer>, Integer>() {
            @Override
            public Iterator<Integer> call(List<Integer> InList) throws Exception {

                ArrayList<Integer> list = new ArrayList<>();

                int num = 0;
                for (int i = 0; i < InList.size(); i++) {
                    num = InList.get(i) * 2;
                    list.add(num);
                }

                return list.iterator();
            }
        });





        flatMapJavaRDD.collect().forEach(System.out::println);


        //4、关闭
        context.close();
    }
}
