package com.stephen.learning.flink;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;

import java.util.ArrayList;

/**
 * @author jack
 * @description 通过sql统计
 * @date 2020/5/8 23:30
 */
public class SQLWordCount {
    public static void main(String[] args) throws Exception{
        //创建上下文环境
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        final BatchTableEnvironment tableEnv = BatchTableEnvironment.create(env);

        String words = "hello flink hello world";
        String[] split = words.split("\\W+");

        ArrayList<WordWithCount> list = new ArrayList<>();
        for(String word:split){
            WordWithCount wc = new WordWithCount(word,1);
            list.add(wc);
        }

        //DataSet转sql,指定字段名
        DataSet<WordWithCount> input = env.fromCollection(list);
        Table table = tableEnv.fromDataSet(input,"word,frequency");
        table.printSchema();
        //注册成表
        tableEnv.createTemporaryView("WordCount",table);
        table = tableEnv.sqlQuery("select word as word, sum(frequency) as frequency from WordCount group by word");

        //将结果转化为DataSet
        DataSet<WordWithCount> output = tableEnv.toDataSet(table,WordWithCount.class);
        output.print();
    }


    public static class WordWithCount {
        public String word;
        public long frequency;

        public WordWithCount() {
        }

        public WordWithCount(String word, long count) {
            this.word = word;
            this.frequency = count;
        }

        @Override
        public String toString() {
            return "WordWithCount{" +
                    "word='" + word + '\'' +
                    ", frequency=" + frequency +
                    '}';
        }
    }
}
