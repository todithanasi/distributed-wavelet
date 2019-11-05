package de.tuberlin.dima.bdapro;

import org.apache.flink.api.java.tuple.Tuple6;

/**
 * Class to represent a point (data stream instance) in the F-Shift algorithm
 * Extends Tuple6 for efficiency compared to simple POJO classes
 * @param <K> The value type e.g. double
 * @param <L> The timestamp type e.g. long
 * @param <W> The shift value type e.g. ShiftValue custom class
 */
public class Point<K, L, W> extends Tuple6<K, K, L, L, L, W> {
    /**
     * Constructor given point's details
     * @param x The x value of the F-Shift algorithm
     * @param l The l value of the F-Shift algorithm
     * @param n The n value of the F-Shift algorithm
     * @param mod The result of timestamp - (timestamp % (n*2)) needed for the composite key
     * @param timestamp The timestamp of the point that showcases the point's order in the stream
     * @param b The shift value that might have been produced along with the point
     */
    public Point(K x, K l, L n, L mod, L timestamp, W b) {
        super(x, l, n, mod, timestamp, b);
    }

    /**
     * Constructor for empty points
     */
    public Point() {
        super();
    }
}
