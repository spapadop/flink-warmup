package de.tuberlin.dima.bdapro.solutions.gameoflife;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class GameOfLifeTaskImpl implements GameOfLifeTask {
    private static int rows;
    private static int cols;

    public GameOfLifeTaskImpl() {
        int sol = solve("./warmup-gol/src/main/resources/inputFile.txt", 100,100,5);
        System.out.println("Alive cells: " + sol);
    }

    @Override
    public int solve(String inputFile, int argN, int argM, int numSteps) {
        rows = argN; // to provide universal access
        cols = argM;

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //read input file to get all current alive cells. Input line example: 1 2
        DataSet<Tuple2<Integer, Integer>> input = env
				.readCsvFile(inputFile).fieldDelimiter(" ")
                .types(Integer.class, Integer.class);

        try {
            // == Begin iteration.
            IterativeDataSet<Tuple2<Integer, Integer>> iterate = input.iterate(numSteps);

            // check what happens with the cells that are alive (count their neighbours)
            DataSet<Tuple2<Integer, Integer>> aliveOnes = iterate.reduceGroup(new checkTheAlive());
            // check what happens with the neighbour cells of the active ones, possibly to get reborn.
            DataSet<Tuple2<Integer, Integer>> deadOnes = iterate.reduceGroup(new checkTheDead());

            // union the aliveOnes that remained alive + the deadOnes that got reborn
            DataSet<Tuple2<Integer, Integer>> finalAlives = aliveOnes.union(deadOnes);
            DataSet<Tuple2<Integer, Integer>> result = iterate.closeWith(finalAlives);
            // == Finished iterations.

            Long output = result.count();
            return output.intValue();

        } catch (Exception e) {
            e.printStackTrace();
        }

        return 0;
    }

    private static class checkTheDead implements GroupReduceFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> {

        @Override
        public void reduce(Iterable<Tuple2<Integer, Integer>> iterable, Collector<Tuple2<Integer, Integer>> collector) throws Exception {

            List<Tuple2<Integer, Integer>> grid = new ArrayList<>();
            List<Tuple2<Integer, Integer>> reborn = new ArrayList<>();

            for (Tuple2<Integer, Integer> t : iterable) {
                grid.add(t);
            }

            // grid has all current alive cells
            for (Tuple2<Integer, Integer> t : grid) {
                List<Tuple2<Integer, Integer>> neighbours = getNeighbours(t.f0, t.f1);

                for (Tuple2<Integer, Integer> neigh : neighbours) {
                    List<Tuple2<Integer, Integer>> neighboursOfDeadCell = getNeighbours(neigh.f0, neigh.f1);

                    int totalNeighbours = 0;
                    for (Tuple2<Integer, Integer> tuple2 : neighboursOfDeadCell) {
                        if (grid.contains(new Tuple2<>(tuple2.f0, tuple2.f1))) {
                            totalNeighbours++;
                        }
                    }

                    if (totalNeighbours == 3 && !grid.contains(new Tuple2<>(neigh.f0, neigh.f1)) && !reborn.contains(new Tuple2<>(neigh.f0, neigh.f1))) {
                        reborn.add(new Tuple2<>(neigh.f0, neigh.f1));
                        collector.collect(new Tuple2<>(neigh.f0, neigh.f1));
                    }
                }
            }
        }

    }


    private class checkTheAlive implements GroupReduceFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> {

        @Override
        public void reduce(Iterable<Tuple2<Integer, Integer>> iterable, Collector<Tuple2<Integer, Integer>> collector) throws Exception {
            List<Tuple2<Integer, Integer>> grid = new ArrayList<>();

            for (Tuple2<Integer, Integer> t : iterable) {
                grid.add(t);
            }

            for (Tuple2<Integer, Integer> t : grid) {
                List<Tuple2<Integer, Integer>> neighbours = getNeighbours(t.f0, t.f1);
                int totalNeighbours = 0;
                for (Tuple2<Integer, Integer> neigh : neighbours) {
                    int curRow = neigh.f0;
                    int curCol = neigh.f1;
                    if (grid.contains(new Tuple2<>(curRow, curCol))) {
                        totalNeighbours++;
                    }
                }

                if (totalNeighbours == 2 || totalNeighbours == 3) {
                    collector.collect(t);
                }
            }

        }

    }


    // *********** HELPER FUNCTIONS ***********

    private static List<Tuple2<Integer, Integer>> getNeighbours(Integer row, Integer col) {
        List<Tuple2<Integer, Integer>> list = new ArrayList<>();
        int rowUp = checkRow(row - 1);
        int rowDown = checkRow(row + 1);
        int colLeft = checkCol(col - 1);
        int colRight = checkCol(col + 1);

        list.add(new Tuple2<>(rowUp, col));
        list.add(new Tuple2<>(rowUp, colLeft));
        list.add(new Tuple2<>(rowUp, colRight));
        list.add(new Tuple2<>(row, colLeft));
        list.add(new Tuple2<>(row, colRight));
        list.add(new Tuple2<>(rowDown, col));
        list.add(new Tuple2<>(rowDown, colLeft));
        list.add(new Tuple2<>(rowDown, colRight));

        return list;
    }

    private static int checkCol(int j) {
        if (j < 0)
            j = cols - 1;
        if (j >= cols)
            j = 0;

        return j;
    }

    private static int checkRow(int i) {
        if (i < 0)
            i = rows - 1;
        if (i >= rows)
            i = 0;

        return i;
    }

}