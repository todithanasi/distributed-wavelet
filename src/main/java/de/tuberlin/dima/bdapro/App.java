package de.tuberlin.dima.bdapro;

import org.apache.flink.api.java.utils.ParameterTool;

import java.io.FileWriter;
import java.text.SimpleDateFormat;
import java.util.Date;


/**
 * Class that is responsible for the general workflow e.g. argument parsing, log writing, benchmarking, etc.
 */
public class App
{

    /**
     * The main method of the F-Shift program
     * @param args The input parameters for the F-Shift algorithm
     * @throws Exception Throws exception if log files cannot be created
     */
    public static void main( String[] args ) throws Exception {
        // We need to consider even the argument name e.g. --output, as a separate argument and count it
        if(args == null || args.length < 12) {
            System.out.println("File Parallelism, Parallelism, input path, output folder path, error delta, timeout in " +
                    "ms, benchmarking (1 for true, 0 for false), number of warm-up rounds, execution rounds and height " +
                    "of the tree required as programme arguments with handles " +
                    "--fileParallelism, --parallelism, --input, --output, --delta, --timeout, --benchmarking, --warmup, " +
                    "--iterations, --height, --pointsThreshold and --delay respectively.");
            System.exit(0);
        }

        ParameterTool parameters = ParameterTool.fromArgs(args);

        String outputPath = parameters.get("output");

        FileWriter fileWriter = new FileWriter(outputPath + "Logs.log", true);
        FileWriter fileWriterExact = new FileWriter(outputPath + "ExactLogs.log", true);

        Algorithm gm = new Algorithm();
        if(parameters.get("benchmarking").equals("1")) {
            int iter = Integer.valueOf(parameters.get("iterations"));
            int warms = Integer.valueOf(parameters.get("warmup"));
            long startTime;
            // Warmup rounds
            for(int i=0; i<warms; i++) {
                startTime = System.currentTimeMillis();
                fileWriterExact.write("Warm-up iteration " + i + " started at " +  getCurrentTimeStamp() + "\n");

                gm.solve(parameters, 0);

                fileWriterExact.write("Warm-up iteration " + i + " ended at " +  getCurrentTimeStamp()+ "\n");
                fileWriter.write("Warm-up iteration " + i + " - Total time " + (System.currentTimeMillis()-startTime)/1000.0 + "\n");

            }

            // Actual iterations
            for(int i=0; i<iter; i++) {
                startTime = System.currentTimeMillis();
                fileWriterExact.write("Iteration " + i + " started at " +  getCurrentTimeStamp()+ "\n");

                gm.solve(parameters,i);

                fileWriterExact.write("Iteration " + i + " ended at " + getCurrentTimeStamp() + "\n");
                fileWriter.write("Iteration " + i + " - Total time " + (System.currentTimeMillis()-startTime)/1000.0 + "\n");


            }
        } else {
            gm.solve(parameters,0);
        }
        fileWriter.write("***************************************************************\n");
        fileWriterExact.write("***************************************************************\n");
        fileWriter.close();
        fileWriterExact.close();
    }

    /**
     * Method to get the current system time in a simple date format
     * @return The current time formatted in a readable fashion
     */
    private static String getCurrentTimeStamp() {
        return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new Date());
    }

}
