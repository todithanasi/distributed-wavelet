package de.tuberlin.dima.bdapro;

import java.io.Serializable;

/**
 * Class to represent a shift coefficient
 */
public class ShiftValue implements Serializable {
    private double value;
    private long level;
    private long rangeFrom;
    private long rangeTo;

    /**
     * Constructor for setting shift coefficient attributes
     * @param value The value of the shift coefficient
     * @param level The level the coefficient belongs to
     * @param rangeFrom The starting range this coefficient covers [rangeFrom, rangeTo]
     * @param rangeTo The end range this coefficient covers [rangeFrom, rangeTo]
     */
    public ShiftValue(double value, long level, long rangeFrom, long rangeTo) {
        this.value = value;
        this.level = level;
        this.rangeFrom = rangeFrom;
        this.rangeTo = rangeTo;
    }

    /**
     * Default constructor if no values are given
     */
    public ShiftValue() {
        this.value = 0;
        this.level = 0;
        this.rangeFrom = 0;
        this.rangeTo = 0;
    }

    /**
     * Get the coefficient's value
     * @return The coefficient's value
     */
    public double getValue() {
        return value;
    }

    /**
     * Set the coefficient's value
     * @param value The coefficient's value to be set
     */
    public void setValue(double value) {
        this.value = value;
    }

    /**
     * Get the level the coefficient belongs to
     * @return The level the coefficient belongs to
     */
    public long getLevel() {
        return level;
    }

    /**
     * Set the level the coefficient belongs to
     * @param level The level the coefficient belongs to
     */
    public void setLevel(long level) {
        this.level = level;
    }

    /**
     * Get the starting point of this coefficient's range
     * @return The starting point of this coefficient's range
     */
    public long getRangeFrom() {
        return rangeFrom;
    }

    /**
     * Set the starting point of this coefficient's range
     * @param rangeFrom The starting point of this coefficient's range to be set
     */
    public void setRangeFrom(long rangeFrom) {
        this.rangeFrom = rangeFrom;
    }

    /**
     * Get the end point of this coefficient's range
     * @return The end point of this coefficient's range
     */
    public long getRangeTo() {
        return rangeTo;
    }

    /**
     * Set the end point of this coefficient's range
     * @param rangeTo The end point of this coefficient's range to be set
     */
    public void setRangeTo(long rangeTo) {
        this.rangeTo = rangeTo;
    }

    /**
     * Method to print (write to file) shift coefficients in a readable format
     * @return The formatted string describing a Shift object
     */
    @Override
    public String toString() {
        return value + " " + level + " " + rangeFrom + " " + rangeTo;
    }
}
