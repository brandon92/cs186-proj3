package simpledb;

public class IntHistogramHelper {

    int min = Integer.MAX_VALUE;
    int max = Integer.MIN_VALUE;
    
    public IntHistogramHelper() {
    }

    public void updateVal(int v) {
	if (v > max) max = v;
	if (v < min) min = v;
    }
    public IntHistogram create(int buckets) {
	return new IntHistogram(buckets, min, max);
    }
}