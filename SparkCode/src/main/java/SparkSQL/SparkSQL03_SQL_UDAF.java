package SparkSQL;

import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Aggregator;

import java.io.Serializable;

/**
 * @Auther:huacishu
 * @Date: 2024/12/23
 */
public class SparkSQL03_SQL_UDAF {
    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession
                .builder()
                .appName("spark01")
                .master("local[2]")
                .getOrCreate();


        //读取数据
        Dataset<Row> ds = sparkSession.read().json("D:\\Work Space\\Learn_Code\\data\\user.json");
        ds.createOrReplaceTempView("user");

        //SparkSQL 采用特殊的方式将UDAF转换成UDF使用
        //UDAF使用时需要创建自定义聚合对象
        //      udaf方法需要传递2个参数
        //          第一个参数表示UDAF对象
        //          第二个参数表示输入编码
        //
        sparkSession.udf().register("myAvg", functions.udaf(
                new MyAvgAgeUDAF(),Encoders.LONG()
        ));


        String sql = "select myAvg(age) from user";
        Dataset<Row> sqlDS = sparkSession.sql(sql);

        //展示数据模型的效果
        sqlDS.show();

        sparkSession.close();
    }
}

