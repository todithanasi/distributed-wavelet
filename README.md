# Implementing Distributed Unrestricted Wavelet Synopses on Data Streams.

## Summary:
The exponential growth of IoT data in the past years has created the need for solutions to achieve (1) high data compression rates to decrease space requirements and processing time, and (2) minimal quality loss to achieve accurate query approximations. One of the popular solutions is Wavelet Synopses. Wavelets provide a compact representation of data by applying thresholding on coefficients. There are multiple algorithms to build wavelet synopses, but our project focuses on F-Shift, an algorithm introduced by Pang, et al. (2009). For more details on the algorithm, its contributions, and the implementation, refer to our final [report](report/Report.pdf). During this project we delivered the following:

 - Explanation of the main concepts introduced in the paper more understandably.
 - Implementation of the centralized version of the algorithm.
 - Adjustments to the centralized version to fit in a distributed environment.
 - Implementation of the distributed version of the algorithm.
 - Benchmarking in terms of result quality (compression rate, error boundness, synopsis quality) and time efficiency (speedup for different data sized and parallelism).

Additionally, to the requested deliverables, we also delivered the following:

 - Enrichment of the centralized and distributed version of F-Shift with additional metadata to allow for reconstruction.
 - Implementation of the wavelet reconstruction method, which was not present in the original paper.
 - Implementation of data generators for benchmarking on synthetic datasets that follow Zipfian or Uniform distribution. We based this implementation on various online resources.

## F-Shift

This is a Flink implementation of the F-Shift algorithm, introduced by Pang, et al. (2009). It includes two main methods, one for the execution of the algorithm in `App.java`, and one for the reconstruction of the data stream in `AppReconstruction.java`.

To run the `App.java`, use the following program arguments:
`bin/flink run -c de.tuberlin.dima.bdapro.App app/f-shift-2.0-SNAPSHOT.jar --parallelism 200 --delta 8 --input /share/flink/flink-1.7.1-wavelets/flink-1.7.1/input/input-zipfian-2.0-4194304 --output /share/flink/flink-1.7.1-wavelets/flink-1.7.1/output/input-zipfian-2.0-4194304-p200-d8-t10000- --height 22 --benchmarking 1 --warmup 1 --iterations 5 --timeout 10000 --fileParallelism 1
--delay 1000 --pointsThreshold 50000`, where:
 - **parallelism** specifies the parallelism of the Flink program
 - **delta** specifies the error-bound
 - **input** specifies the input file (data stream) to be processed
 - **output** specifies the path to the output directory to write the shift coefficients
 - **height** specifies the height of the final tree and depends on the number of points in the input file (height = log_2(n))
 - **benchmarking** specifies whether we need to run in benchmarking mode (1) or not (0)
 - **warmup** specifies the number of executions that are not taken into consideration for benchmarking
 - **iterations** specifies the number of iterations for benchmarking
 - **timeout** specifies the time period of inactivity that signals the end of the iteration in Flink
 - **fileParallelism** specifies the output file parallelism
 - **delay** specifies the number of ms to wait before processing the next batch of points
 - **pointsThreshold** specifies the points' ingestion rate for the stream processing (throttle)

To run the `AppReconstruction.java`, use the following program arguments:
`bin/flink run -c de.tuberlin.dima.bdapro.AppReconstruction app/f-shift-2.0-SNAPSHOT.jar --parallelism 200 --delta 8 --input /share/flink/flink-1.7.1-wavelets/flink-1.7.1/output/input-zipfian-2.0-4194304-p200-d8-t10000- --output /share/flink/flink-1.7.1-wavelets/flink-1.7.1/reconstruction/input-zipfian-2.0-4194304-p200-d8-t10000- --height 22 --benchmarking 1 --warmup 1 --iterations 5`, where:
 - **parallelism** specifies the parallelism of the Flink program
 - **delta** specifies the error-bound
 - **input** specifies the input path where the shifts file can be found
 - **output** specifies the path to the output directory to write the reconstructed points
 - **height** specifies the height of the final tree and depends on the number of points in the input file (height = log_2(n))
 - **benchmarking** specifies whether we need to run in benchmarking mode (1) or not (0)
 - **warmup** specifies the number of executions that are not taken into consideration for benchmarking
 - **iterations** specifies the number of iterations for benchmarking

### References:

[1] Stollnitz et al. Wavelets for computer graphics: a primer. 1. IEEE Computer Graphics and Applications, 1995.

[2] Yossi Matias et al. Wavelet-based histograms for selectivity estimation. SIGMOD, 1998.

[3] M. Garofalakis and P. B. Gibbons. Wavelet synopses with error guarantees. In Proceedings of the 2002 ACM SIGMOD international conference on Management of data, pages 476–487. ACM, 2002.

[4] M. Garofalakis and A. Kumar. Deterministic wavelet thresholding for maximum-error metrics. In Proceedings of the twenty-third ACM SIGMOD-SIGACT-SIGART symposium on Principles of database systems, pages 166–176. ACM, 2004.

[5] Pang, Chaoyi, et al. "Unrestricted wavelet synopses under maximum error bound." EDBT 2009.

[6] Pang, Chaoyi, et al. "Computing unrestricted synopses under maximum error bound." Algorithmica 65.1, 2013.