package simpledb;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    private int[] buckets;
    private int width;
    private int nTups;
    private int min;
    private int max;


    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
    	// some code goes here
	this.buckets = new int[buckets];
	this.width = (int) Math.ceil((max - min + 1)/ (double) buckets);
	this.min = min;
	this.max = max;
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	// some code goes here
	buckets[bucketOf(v)] += 1;
	nTups += 1;
    }

    /** Helper method that determines what bucket an integer value belongs to. */
    private int bucketOf(int v) {
	int b = (v - min) / width; // Integer should floor divide
	return b;
    }

    /** Helper method that determines a buckets selectivity contribution */
    private double bucketSelect(int bucketIndex) {
	if (bucketIndex < 0 || bucketIndex >= buckets.length) return 0;
	return buckets[bucketIndex] / (double) (nTups);
    }

    /** Helper method that determines the fractional contribution of the
	bucket that contains a portion greater than a constant. */
    private double bucketGreat(int c) {
	int bucketIndex = bucketOf(c);
	int bRight = (width * (bucketIndex + 1)) - 1 + min;
	return (bRight - c) / (double) (width);
    }

    /** Helper that determines the fractional contribution of the
	bucket that contains a portion less than a constant */
    private double bucketLess(int c) {
	int bucketIndex = bucketOf(c);
	int bLeft = (width * bucketIndex) + min;
	return (c - bLeft) / (double) (width);
    }

    /** Returns the selectivity of an equality expression f=const*/
    private double bEquals(int c) { 
	return bucketSelect(bucketOf(c)) / width;
    }

    /** Returns the selectivity of a less than expression. */
    private double bLess(int c) {
	double result = bucketLess(c);
	for (int i = bucketOf(c) - 1; i >= 0; i--) {
	    result += bucketSelect(i);
	}
	return result;
    }

    /** Returns the selectivity of a greater than expression. */
    private double bGreat(int c) {
	double result = bucketGreat(c);
	for (int i = bucketOf(c) + 1; i < buckets.length; i++) {
	    result += bucketSelect(i);
	}
	return result;
    }

    /** Returns the selectivity of a bucket. */

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {

    	// some code goes here
	double result = 0;

	int currBucket = bucketOf(v);
	switch(op) {
	case LIKE:
	case EQUALS:
	    if (v < min || v > max) return 0;
	    return bEquals(v);
	case GREATER_THAN:
	    if (v >= max) return 0;
	    else if (v < min) return 1;
	    return bGreat(v);
	case LESS_THAN:
	    if (v > max) return 1;
	    else if (v <= min) return 0;
	    return bLess(v);
	case LESS_THAN_OR_EQ:
	    if (v > max) return 1;
	    else if (v < min) return 0;
	    return bLess(v) + bEquals(v);
	case GREATER_THAN_OR_EQ:
	    if (v > max) return 0;
	    else if (v < min) return 1;
	    return bGreat(v) + bEquals(v);
	case NOT_EQUALS:
	    if (v > max || v < min) return 1;
	    return 1 - bucketSelect(bucketOf(v));
	default:
	    throw new IllegalStateException("impossible to reach here");	 
	}
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // some code goes here
        return 1.0;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {

        // some code goes here
        return null;
    }
}
