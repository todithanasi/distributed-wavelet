package de.tuberlin.dima.bdapro;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 *
 * This class implements the F-Shift algorithm
 * Note that:
 * Point.f0 = x
 * Point.f1 = l
 * Point.f2 = n
 * Point.f3 = mod
 * Point.f4 = timestamp
 * Point.f5 = b
 */
class Algorithm {
    private static int height, delta;
    private static int iteratorTimeout;
    private static int pointsThreshold, delay;


    /**
     * Method that implements the F-Shift logic in Flink
     * @param parameters The input parameters (program arguments)
     * @param i The iteration number if benchmarking is enabled, otherwise 0 (dummy)
     * @throws Exception Throws exception if input file is not found
     */
    void solve(ParameterTool parameters, int i) throws Exception {

        // Set execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(Integer.valueOf(parameters.get("parallelism")));
        // Read parameters
        String inputFile = parameters.get("input");
        String outputFile = parameters.get("output") + i +  "/shifts.txt";
        delta = Integer.valueOf(parameters.get("delta"));
        height = Integer.valueOf(parameters.get("height"));
        iteratorTimeout = Integer.valueOf(parameters.get("timeout"));
        int fileParallelism = Integer.valueOf(parameters.get("fileParallelism"));
        pointsThreshold = Integer.valueOf(parameters.get("pointsThreshold"));
        delay = Integer.valueOf(parameters.get("delay"));
        // Initialize stream
        DataStream<String> someIntegers = env
                .readFile(new TextInputFormat(new org.apache.flink.core.fs.Path(inputFile)),inputFile);
        //DataStream<String> someIntegers = env.socketTextStream("localhost", 9999);
        // System.out.println(inputFile);

        DataStream<String> someIntegersWithDelay = someIntegers.map(new AddDelay(pointsThreshold, delay) );

        // Convert the integers read from file to stream of points
        DataStream<Point<Double, Long, ShiftValue>> initialPoints = someIntegersWithDelay
                .map(new mapToRealValue());


        // initialPoints.print();
        // Implement the algorithm's logic
        IterativeStream<Point<Double, Long, ShiftValue>> iteration = initialPoints.iterate(iteratorTimeout);
        DataStream<Point<Double, Long, ShiftValue>> nextGeneration = iteration
                .keyBy(2,3)
                .countWindow(2,2)
                .reduce(new reduceToOnePoint(delta));
        DataStream<Point<Double, Long, ShiftValue>> result = iteration.closeWith(nextGeneration); // The result of each iteration

        // System.out.println(env.getExecutionPlan());
        DataStream<ShiftValue> finalValues = result.flatMap(new ShiftValues(height, delta));

        finalValues.writeAsText(outputFile, FileSystem.WriteMode.OVERWRITE).setParallelism(fileParallelism);

        // finalValues.print();

        env.execute("F-Shift Stream");
    }

    /**
     * Class to convert lines from the input file to Point objects
     */
    private class mapToRealValue implements MapFunction<String,Point<Double, Long, ShiftValue>> {

        // Use a common instance of the point class, in order to decrease pressure on the garbage collector
        // f0 (x) contains a dummy value
        private Point<Double, Long, ShiftValue> result = new Point<>(0.0,0.0,1L,0L, 0L, new ShiftValue());

        /**
         * Method to convert lines from the input file to Point objects
         * @param value The input line in a string format
         * @return The Point object containing the necessary information for the F-Shift algorithms
         */
        @Override
        public Point<Double, Long, ShiftValue> map(String value) {
            // Set dummy f0 (x) to its real value
            result.f0 = Double.parseDouble(value.split(" ")[1]);
            int timestamp = Integer.valueOf(value.split(" ")[0]);
            result.f4 = (long) timestamp;
            result.f3 = (long) timestamp - (timestamp % (result.f2*2));
            result.f5.setRangeFrom(timestamp);
            result.f5.setRangeTo(timestamp);
            //Thread.sleep(0,100);

            return result;
        }
    }

    /**
     * Class to combine two points of level n-1 to a single point for level n, where n=1,...,log(#points)
     */
    private class reduceToOnePoint implements ReduceFunction<Point<Double, Long, ShiftValue>> {

        private Point<Double, Long, ShiftValue> result = new Point<>();

        double d_g;
        double d_s;
        ShiftValue b;
        final int delta;

        /**
         * Constructor to pass the global parameter delta (error bound)
         * @param delta The error bound
         */
        reduceToOnePoint(int delta) {
            this.delta = delta;
        }

        /**
         * Method to combine two points of level n-1 to a single point for level n, where n=1,...,log(#points)
         * @param f_l The left point
         * @param f_r The right point
         * @return The resulting Point object after applying the F-Shift logic
         */
        @Override
        public Point<Double, Long, ShiftValue> reduce(Point<Double, Long, ShiftValue> f_l, Point<Double, Long, ShiftValue> f_r) {

            d_g = Math.max(f_l.f0+f_l.f1, f_r.f0+f_r.f1);
            d_s = Math.min(f_l.f0-f_l.f1, f_r.f0-f_r.f1);

            if (d_g - d_s >= 2 * delta) { // new shift value needs to be created
                // if elements are coming out of order (timestamp of f_L > timestamp of f_R) change the order
                if (f_l.f4 > f_r.f4) {
                    b = new ShiftValue((f_r.f0-f_l.f0)/2.0, f_r.f5.getLevel() + 1,
                            Math.min(f_l.f5.getRangeFrom(),f_r.f5.getRangeFrom()),
                            Math.max(f_l.f5.getRangeTo(), f_r.f5.getRangeTo()));
                    result.f0 = f_r.f0-b.getValue(); // x
                } else {
                    b = new ShiftValue((f_l.f0-f_r.f0)/2.0, f_l.f5.getLevel() + 1,
                            Math.min(f_l.f5.getRangeFrom(),f_r.f5.getRangeFrom()),
                            Math.max(f_l.f5.getRangeTo(), f_r.f5.getRangeTo()));
                    result.f0 = f_l.f0-b.getValue(); // x
                }

                result.f1 = Math.max(f_l.f1,f_r.f1); // l
            }
            else { // no new shift value produced
                b = new ShiftValue(0, f_l.f5.getLevel() + 1,
                        Math.min(f_l.f5.getRangeFrom(),f_r.f5.getRangeFrom()),
                        Math.max(f_l.f5.getRangeTo(), f_r.f5.getRangeTo()));
                result.f1 = (d_g-d_s)/2.0; // l
                result.f0 = (d_g+d_s)/2.0; // x
            }

            // Prepare the resulting point; use class variable (overwrite) for efficiency, instead of creating a new one every time
            result.f2 = 2*f_l.f2;
            result.f4 = f_l.f3;
            result.f3 = result.f4 - (result.f4 % (result.f2 * 2)); //timestamp - timestamp % 2*n
            result.f5 = b;
            return result;

        }
    }

    /**
     * Class to collect the shift values for writing to file
     */
    private static class ShiftValues implements FlatMapFunction<Point<Double, Long, ShiftValue>, ShiftValue> {
        final int delta;
        final int height;

        /**
         * Constructor to pass the global parameters height and delta
         * @param height The expected height of the tree in order to identify the last B value at the last level
         *               (since streaming is technically infinite we need such restriction)
         * @param delta The error bound
         */
        ShiftValues(int height, int delta) {
            this.delta = delta;
            this.height = height;
        }

        /**
         *
         * @param doubleLongShiftValuePoint A point that may or may not contain a shift value to be written to file
         * @param collector The resulting shift values to be collected
         */
        @Override
        public void flatMap(Point<Double, Long, ShiftValue> doubleLongShiftValuePoint, Collector<ShiftValue> collector) {
            if(doubleLongShiftValuePoint.f5.getValue() != 0) {
                collector.collect(doubleLongShiftValuePoint.f5);
            }

            // To get the last shift value (potentially) at the root
            if(doubleLongShiftValuePoint.f5.getLevel() == height) {
                if(Math.abs(doubleLongShiftValuePoint.f0) >= Math.abs(delta-doubleLongShiftValuePoint.f1)) {
                    doubleLongShiftValuePoint.f5.setLevel(doubleLongShiftValuePoint.f5.getLevel() + 1);
                    doubleLongShiftValuePoint.f5.setValue(doubleLongShiftValuePoint.f0);
                    collector.collect(doubleLongShiftValuePoint.f5);
                }
            }
        }
    }

    /**
     * Class to add delay after a batch of points (throttle) as a workaround for the Flink iteration bug
     */
    private static class AddDelay implements MapFunction<String, String> {
        int delay;
        int pointsThreshold;
        int count;

        /**
         * Constructor for setting the class variables
         * @param pointsThreshold The number of points after which the throttle is inserted
         * @param delay The delay (throttle) we insert
         */
        AddDelay(int pointsThreshold, int delay) {
            this.delay = delay;
            this.pointsThreshold = pointsThreshold;
        }

        /**
         * Map function to enable the counting of points (input lines) after which we insert the throttle
         * @param s The input line read from the file
         * @return The input line read from file unchanged
         * @throws Exception Throws exception if thead cannot sleep
         */
        @Override
        public String map(String s) throws Exception {
            count++;
            if(count == pointsThreshold){
                count=0;
                Thread.sleep(delay);
            }

            return s;
        }
    }
}