package de.tuberlin.dima.bdapro;

import scala.Tuple2;

/**
 * Class to represent a reconstructed point (less detailed than Point class)
 * Extends Tuple2 for efficiency
 * @param <K> The value type e.g. double
 * @param <V> The timestamp type e.g. long
 */
public class ReconstructedPoint<K,V> extends Tuple2<K,V> {
    /**
     * Constructor given a reconstructed point's details
     * @param approximateValue The approximated value after reconstruction
     * @param timestamp The timestamp this value refers to
     */
    public ReconstructedPoint(K approximateValue, V timestamp) {
        super(approximateValue, timestamp);
    }

    /**
     * Method to print (or write to file) a class object in a specific format
     * @return The formatted string describing a ReconstructedPoint object
     */
    @Override
    public String toString() {
        return this._2 + " " + this._1;
    }

}
