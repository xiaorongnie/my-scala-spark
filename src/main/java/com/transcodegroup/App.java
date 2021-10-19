package com.transcodegroup;


import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.util.LongAccumulator;

import java.util.Arrays;
import java.util.List;

/**
 * 入口函数
 * @author eason
 */

@Data
@Slf4j
public class App  {

    public static void main(String[] args) {
        JavaSparkContext sc = null;
        try {
            //初始化 JavaSparkContext
            SparkConf conf = new SparkConf().setMaster("local").setAppName("wordCountTest");
            sc = new JavaSparkContext(conf);

            //构造数据源
            List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);

            //并行化创建rdd
            JavaRDD<Integer> rdd = sc.parallelize(data);

            //累加器
            final LongAccumulator accumulator = sc.sc().longAccumulator();

            rdd.foreach((VoidFunction<Integer>) integer -> accumulator.add(integer));

            System.out.println("执行结果：" + accumulator.value());
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (sc != null) {
                sc.close();
            }
        }
    }


}
