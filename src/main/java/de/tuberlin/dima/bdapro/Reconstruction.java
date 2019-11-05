package de.tuberlin.dima.bdapro;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Class for reconstructing the input data stream based on shift coefficients
 * Not included in the requirements or paper description, but implemented for the quality benchmarking
 */
public class Reconstruction {

    private final ExecutionEnvironment env;
    private final HashMap<Integer,ArrayList<ShiftValue>> shifts;
    private String shiftsPath;
    private String reconstructionPath;
    private String inputPath;
    private String qualityPath;
    private final int height;

    /**
     * Constructor for setting the class variables' values
     * @param parameters The input parameters required for reconstruction
     * @param i The iteration round if benchmarking is enabled, otherwise 0 (dummy)
     */
    public Reconstruction(ParameterTool parameters, int i) {
        env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(Integer.valueOf(parameters.get("parallelism")));
        shifts = new HashMap<>();
        String outputFolder = parameters.get("output");
        shiftsPath = outputFolder + i +  "/shifts.txt";
        reconstructionPath = outputFolder + i +  "/reconstruction.txt";
        inputPath = parameters.get("input");
        qualityPath = outputFolder + i +  "/quality.txt";
        height = Integer.valueOf(parameters.get("height"));
    }

    /**
     * Method for reconstructing the input data points based on shift coefficients stored in a file
     * @throws Exception Throws exception if the shifts file cannot be found
     */
    void reconstructFromFile() throws Exception {
        // Variables to identify range from file
        long minRange = Long.MAX_VALUE;
        long maxRange = -1;

        try(BufferedReader br = new BufferedReader(new FileReader(shiftsPath))) {
            String line = br.readLine();
            // While there are more shift values
            while (line != null) {
                String[] values = line.split(" ");

                // Check if min and max range change
                /* The only input needed is the path where the B-list is found.
                 *  The whole range you can identify while reading the file. */
                if(Integer.valueOf(values[2]) < minRange) {
                    minRange = Integer.valueOf(values[2]);
                }
                if(Integer.valueOf(values[3]) > maxRange) {
                    maxRange = Integer.valueOf(values[3]);
                }

                int level = Integer.valueOf(values[1]);
                ArrayList<ShiftValue> shiftValues;
                if(!shifts.containsKey(level)) { // If this is the first shift value of the level
                    shiftValues = new ArrayList<>();
                } else { // If the level is already there
                    shiftValues = shifts.get(level);
                }
                shiftValues.add(new ShiftValue(Double.valueOf(values[0]), Long.valueOf(values[1]),
                        Long.valueOf(values[2]), Long.valueOf(values[3])));
                // Add the level and list of shift coefficients to the map
                shifts.put(level, shiftValues);
                line = br.readLine();
            }
            reconstructFromMap(shifts, minRange, maxRange, height);
        }
    }

    /**
     * Method for reconstructing the input data points based on shift coefficients stored in a Map data structure
     * @param B The map of shift values; the format is (key=level, value=list of level's shift coefficients)
     * @param rangeFrom The minimum timestamp in the data
     * @param rangeTo The maximum timestamp in the data
     * @param height The height of the tree (log(#points))
     */
    private void reconstructFromMap(HashMap<Integer, ArrayList<ShiftValue>> B, long rangeFrom, long rangeTo, int height) throws Exception {
        // Calculate the final level to be checked
        int rootLevel = (int)(Math.log(rangeTo - rangeFrom + 1)/Math.log(2)) + 1;

        DataSet<Long> timestamps = env.generateSequence(rangeFrom,rangeTo);

        DataSet<ReconstructedPoint<Double, Long>> points = timestamps
                .map(new MapFunction<Long, ReconstructedPoint<Double, Long>>() {
                    /**
                     * Method to reconstruct a point given a set of shift coefficients
                     * @param timestamp The timestamp for which we want to reconstruct
                     * @return The reconstructed value for the given timestamp
                     */
                    @Override
                    public ReconstructedPoint<Double, Long> map(Long timestamp) {
                        int level = 1;
                        double reconstructedValue = 0;
                        // FOR EACH LEVEL ADJUST THE RECONSTRUCTED VALUE
                        // If the level key is not included, it means that we don't have more levels
                        while (level <= height + 1) {
                            if(B.containsKey(level)) {
                                ArrayList<ShiftValue> shiftsOfLevel = B.get(level);
                                for (ShiftValue s : shiftsOfLevel) {
                                    // The timestamp falls within the range
                                    if (timestamp >= s.getRangeFrom() && timestamp <= s.getRangeTo()) {
                                        double middle = (s.getRangeFrom() + s.getRangeTo()) / 2.0;
                                        if (timestamp < middle || level == rootLevel) {
                                            reconstructedValue += s.getValue();
                                        } else {
                                            reconstructedValue -= s.getValue();
                                        }
                                        break; // no need to check more ranges in this level
                                    }
                                }
                            }
                            level += 1;
                        }
                        return new ReconstructedPoint<Double,Long>(reconstructedValue, timestamp);
                    }

                });
        points.writeAsText(reconstructionPath, FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        env.execute();

        // points.print();
    }

    /**
     * Method for checking the quality of the results, namely the reconstruction error abs(actual point-reconstructed point)
     * @throws Exception Throws exception if the output file cannot be created
     */
    void getQuality () throws Exception {

        System.out.println("Checking quality of results");

        DataSet<ReconstructedPoint<Double,Long>> reconstructions = env.readTextFile(reconstructionPath)
                .map(new mapToReconstructedPoint());
        DataSet<ReconstructedPoint<Double,Long>> realValues = env.readTextFile(inputPath)
                .map(new mapToReconstructedPoint());
        DataSet<ReconstructedPoint<Double,Long>> allPairs = reconstructions.union(realValues);

        DataSet<ReconstructedPoint<Double,Long>> differences = allPairs
                .groupBy((KeySelector<ReconstructedPoint<Double, Long>, Long>) rc -> rc._2)
                .reduce(new calculateDifference());

        differences.writeAsText(qualityPath, FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        env.execute();
    }

    /**
     * Class to facilitate the reading of reconstructed points from file during the quality checking
     */
    private class mapToReconstructedPoint implements MapFunction<String, ReconstructedPoint<Double, Long>> {

        /**
         * Method to convert a file line into a ReconstructedPoint object
         * @param s The line of input file containing (timestamp, reconstructed value) pairs
         * @return The ReconstructedPoint object corresponding to s
         */
        @Override
        public ReconstructedPoint<Double, Long> map(String s) {
            return new ReconstructedPoint<Double, Long>(Double.valueOf(s.split(" ")[1]),
                    Long.valueOf(s.split(" ")[0]));
        }
    }

    /**
     * Class to calculate the difference between an actual point and a reconstructed point for the same timestamp
     */
    private class calculateDifference implements org.apache.flink.api.common.functions.ReduceFunction<ReconstructedPoint<Double, Long>> {
        /**
         * Method to calculate the difference between two points, e.g. actual input data vs reconstructed point
         * @param p1 The first point for the difference
         * @param p2 The second point for the difference
         * @return The absolute difference between the points' values
         */
        @Override
        public ReconstructedPoint<Double, Long> reduce(ReconstructedPoint<Double, Long> p1, ReconstructedPoint<Double, Long> p2) {
            return new ReconstructedPoint<Double,Long>(Math.abs(p1._1-p2._1),p1._2);
        }
    }
}
