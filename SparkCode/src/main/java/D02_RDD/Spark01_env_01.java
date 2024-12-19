package D02_RDD;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * @Auther:huacishu
 * @Date: 2024/6/22
 */
public class Spark01_env_01 {
    public static void main(String[] args) {
    //创建Spark环境
        //1、创建SparkConf对象
        SparkConf conf = new SparkConf();
        //2、加载配置
        conf.setMaster("local[*]");
        conf.setAppName("spark01_env");
        //3、创建SparkContext对象
        JavaSparkContext context = new JavaSparkContext(conf);//无参构造
            //def this() = this(new SparkContext()) 返回了一个SparkContext对象

        //Spark计算引擎，对数据做处理，  1、加载数据  2、处理数据  3、保存结果。



        //4、关闭context
        context.stop();
    }
}
