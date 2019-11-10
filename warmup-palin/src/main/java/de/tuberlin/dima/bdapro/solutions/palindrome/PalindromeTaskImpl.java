package de.tuberlin.dima.bdapro.solutions.palindrome;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.*;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

public class PalindromeTaskImpl implements PalindromeTask {

    public PalindromeTaskImpl() {
        Set<String> result = solve("./warmup-palin/src/main/resources/input.txt");
        for (String r : result){
            System.out.println(r);
        }
    }

    /**
     * Solution using withBroadcast function.
     * Execution time on server: ~2.5 sec
     *
     * @param inputFile
     * @return a collection of distinct longest palindromic sentences
     */
    @Override
    public Set<String> solve(String inputFile) {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<String> inputText = env.readTextFile(inputFile);

        DataSet<Tuple2<String, Integer>> sentencesWithLength = inputText
                .filter(t -> t.length() > 1)
                .flatMap(new IsPalindrome());

        MapOperator<Tuple2<String, Integer>, Integer> max = sentencesWithLength.max(1).map(new ConvertToInt());

        DataSet<Tuple2<String, Integer>> result = sentencesWithLength.filter(new getLongestSentences()).withBroadcastSet(max, "max");

        Set<String> solution = new HashSet<>();
        try {
            List<Tuple2<String, Integer>> list = result.collect();
            for (Tuple2<String, Integer> t : list) {
                solution.add(t.f0);
            }

//            for(String s: solution){ System.out.println(s); }

        } catch (Exception e) {
            e.printStackTrace();
        }

        return solution;
    }

    /**
     * Solution using cross function.
     * Execution time on server: ~ 6.5 sec
     *
     * @param inputFile
     * @return a collection of distinct longest palindromic sentences
     */
    public Set<String> solveWithCross(String inputFile) {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<String> inputText = env.readTextFile(inputFile);

        DataSet<Tuple2<String, Integer>> sentencesWithLength = inputText
                .filter(t -> t.length() > 1)
                .flatMap(new IsPalindrome());

        MapOperator<Tuple2<String, Integer>, Integer> max = sentencesWithLength.max(1).map(new ConvertToInt());

        DataSet<String> proj = sentencesWithLength
                .crossWithTiny(max)
                .filter(new KeepMaxOnly())
                .map(new keepStringOnly());

        Set<String> solution = new HashSet<>();
        try {
            List<String> list = proj.collect();
            for (String t : list) {
                solution.add(t);
            }

//            for(String s: solution){ System.out.println(s); }

        } catch (Exception e) {
            e.printStackTrace();
        }

        return solution;
    }

    /**
     * Solution using simple join.
     * Execution time on server: ~ 7 sec
     *
     * @param inputFile
     * @return a collection of distinct longest palindromic sentences
     */
    public Set<String> solveWithSimpleJoin(String inputFile) {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<String> inputText = env.readTextFile(inputFile);

        DataSet<Tuple2<String, Integer>> sentencesWithLength = inputText
                .filter(t -> t.length() > 1)
                .flatMap(new IsPalindrome());

        AggregateOperator<Tuple2<String, Integer>> max = sentencesWithLength.max(1);

        DistinctOperator<Tuple> proj = sentencesWithLength
                .joinWithTiny(max)
                .where(1)
                .equalTo(1)
                .projectFirst(0)
                .distinct(0);

        Set<String> solution = new HashSet<>();
        try {
            List<Tuple> list = proj.collect();
            for (Tuple t : list) {
                solution.add(t.getField(0));
            }
//            for(String s: solution){ System.out.println(s); }

        } catch (Exception e) {
            e.printStackTrace();
        }

        return solution;
    }

    public static class IsPalindrome implements FlatMapFunction<String, Tuple2<String, Integer>> {

        @Override
        public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
            boolean flag = true;
            String temp = s.toLowerCase().replaceAll("\\s+", "");

            int i = 0;
            int j = temp.length() - 1;
            while (j > i) {
                if (temp.charAt(i) == temp.charAt(j)) {
                    i++;
                    j--;
                } else {
                    flag = false;
                    break;
                }
            }
            if (flag)
                collector.collect(new Tuple2<>(s, temp.length()));
        }
    }

    private class getLongestSentences extends RichFilterFunction<Tuple2<String, Integer>> {

        private int max;

        @Override
        public void open(Configuration parameters) throws Exception {
            this.max = (Integer) getRuntimeContext().getBroadcastVariable("max").get(0);
            System.out.println(this.max);
        }

        @Override
        public boolean filter(Tuple2<String, Integer> tuple2) throws Exception {
            return tuple2.f1 == this.max;
        }
    }

    private class ConvertToInt implements MapFunction<Tuple2<String, Integer>, Integer> {

        @Override
        public Integer map(Tuple2<String, Integer> tuple2) throws Exception {
            return tuple2.f1;
        }
    }

    private class KeepMaxOnly implements FilterFunction<Tuple2<Tuple2<String, Integer>, Integer>> {
        @Override
        public boolean filter(Tuple2<Tuple2<String, Integer>, Integer> tuple2IntegerTuple2) throws Exception {
            return tuple2IntegerTuple2.f0.f1 == tuple2IntegerTuple2.f1;
        }
    }

    private class keepStringOnly implements MapFunction<Tuple2<Tuple2<String, Integer>, Integer>, String> {


        @Override
        public String map(Tuple2<Tuple2<String, Integer>, Integer> tuple2IntegerTuple2) throws Exception {
            return tuple2IntegerTuple2.f0.f0;
        }
    }

}
