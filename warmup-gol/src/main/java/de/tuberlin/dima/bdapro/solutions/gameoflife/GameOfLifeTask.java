package de.tuberlin.dima.bdapro.solutions.gameoflife;

/**
 * @author Tayfun Wiechert <wiechert@campus.tu-berlin.de>
 */
public interface GameOfLifeTask {

    int solve(String inputFile, int argN, int argM, int numSteps);

}
