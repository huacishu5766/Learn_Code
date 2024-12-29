package SparkSQL;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

/**
 * @Auther:huacishu
 * @Date: 2024/12/23
 */
public class SparkSQL05_Source_Hive {
    public static void main(String[] args) {
        System.setProperty("HADOOP_USER_NAME","huacishu");

        SparkSession sparkSession = SparkSession
                .builder()
                .enableHiveSupport()// 添加hive支持
                .appName("spark01")
                .master("local[2]")
                .getOrCreate();

        //3. 编写代码
        sparkSession.sql("show tables").show();

        sparkSession.sql("create table user_info(name String,age bigint)");
        sparkSession.sql("insert into table user_info values('zhangsan',10)");
        sparkSession.sql("select * from user_info").show();

        sparkSession.close();
    }
}
