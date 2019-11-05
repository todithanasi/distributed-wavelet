package de.tuberlin.dima.bdapro;

import org.apache.flink.api.java.utils.ParameterTool;

/**
 * Class that is responsible for the workflow of the reconstruction of the input data
 */
public class AppReconstruction
{
    /**
     * The main method for data reconstruction
     * @param args The input parameters for reconstruction
     * @throws Exception Throws exception if shifts file cannot be found or output files cannot be created
     */
    public static void main( String[] args ) throws Exception {
        // We need to consider even the argument name e.g. --output, as a separate argument and count it
        if(args == null || args.length < 14) {
            System.out.println("Parallelism, input path, output folder path, error delta, benchmarking (1 for true, 0 " +
                    "for false), execution rounds and height of the tree required as programme arguments with handles " +
                    "--parallelism, --input, --output, --delta, --benchmarking, --iterations, and --height respectively.");
            System.exit(0);
        }

        ParameterTool parameters = ParameterTool.fromArgs(args);

        if(parameters.get("benchmarking").equals("1")) {
            int iter = Integer.valueOf(parameters.get("iterations"));

            // Actual iterations
            for(int i=0; i<iter; i++) {
                Reconstruction rc = new Reconstruction(parameters, i);
                // Reconstruct points
                rc.reconstructFromFile();
                // Get the quality of the reconstruction
                rc.getQuality();
            }
        } else {
            Reconstruction rc = new Reconstruction(parameters, 0);
            // Reconstruct points
            rc.reconstructFromFile();
            // Get the quality of the reconstruction
            rc.getQuality();
        }
    }
}
