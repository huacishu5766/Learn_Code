package SparkSQL;

import org.apache.spark.sql.SparkSession;

/**
 * @Auther:huacishu
 * @Date: 2024/12/24
 */
public class SparkSQL06_Source_Req_1 {
    public static void main(String[] args) {
        //在编码前，设定Hadoop的访问用户
        System.setProperty("HADOOP_USER_NAME","huacishu");

        SparkSession sparkSession = SparkSession
                .builder()
                .enableHiveSupport() // 启用Hive支持
                .appName("spark01")
                .master("local[2]")
                .getOrCreate();

        sparkSession.sql("select\n" +
                "click_product_id,\n" +
                "product_name,\n" +
                "city_name,\n" +
                " area\n" +
                "from\n" +
                "(select\n" +
                "     click_product_id,\n" +
                "     city_id\n" +
                " from user_visit_action\n" +
                " where click_product_id != -1\n" +
                " ) a\n" +
                " join (\n" +
                " select\n" +
                "    product_id,\n" +
                "    product_name\n" +
                " from product_info\n" +
                " ) p on a.click_product_id = p.product_id\n" +
                " join (\n" +
                " select\n" +
                "    city_id,\n" +
                "    city_name,\n" +
                "    area\n" +
                " from city_info\n" +
                " )c on a.city_id = c.city_id\n" +
                " limit 10");

        sparkSession.close();
    }

}
