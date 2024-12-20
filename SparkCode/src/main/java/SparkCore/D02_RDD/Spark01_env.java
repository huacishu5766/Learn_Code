package SparkCore.D02_RDD;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;

/**
 * @Auther:huacishu
 * @Date: 2024/6/22
 */
public class Spark01_env {
    public static void main(String[] args) {
        // 1、创建SparkConf对象，配置信息
        SparkConf conf = new SparkConf();
        conf.setMaster("local[2]");
        conf.setAppName("sprak01");
        // 2、 创建SparkContext对象，上下文环境
        SparkContext sparkContext = new SparkContext(conf);



        // 3、 关闭上下文环境
        sparkContext.stop();
    }
}
