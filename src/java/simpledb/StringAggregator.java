package simpledb;
import java.util.HashMap;
import java.util.Iterator;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;
    private HashMap<Field, Integer> results;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
	this.gbfield = gbfield;
	this.gbfieldtype = gbfieldtype;
	this.afield = afield;
	//if (what != COUNT) {
	//    throw new IllegalArgumentException("StringAggregator only supports COUNT" );
	//}
	this.what = what;
	results = new HashMap<Field, Integer>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
	Field groupField;
	if (gbfield == NO_GROUPING) {
	    groupField = null;
	} else {
	    groupField = tup.getField(gbfield);
	}
	if (!results.containsKey(groupField)) {
	    results.put(groupField, 0);
	}
	results.put(groupField, results.get(groupField) + 1);
    }

    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public DbIterator iterator() {
        // some code goes here
	// throw new UnsupportedOperationException("please implement me for proj2");
	return new resultIter();
    }

    private class resultIter implements DbIterator {
	boolean illegal = true;
	TupleDesc td;
	Iterator<Field> fieldIter;
	public resultIter() { 
	    Type[] typeAr = {gbfieldtype, Type.INT_TYPE};
	    if (gbfieldtype == null) {
		typeAr = new Type[]{Type.INT_TYPE};
	    }
	    td = new TupleDesc(typeAr);
	}

	public void open() throws DbException, TransactionAbortedException {
	    illegal = false;
	    fieldIter = results.keySet().iterator();
	}
       
	public boolean hasNext() throws DbException, TransactionAbortedException {
	    if (illegal) throw new IllegalStateException();
	    return fieldIter.hasNext();
	}
	public Tuple next() throws DbException, TransactionAbortedException{
	    if (illegal) throw new IllegalStateException();
	    Field f = fieldIter.next();
	    int count = results.get(f);
	    Tuple t = new Tuple(td);
	    if (gbfieldtype == null) {		
		t.setField(0, new IntField(count));
	    } else {
		t.setField(0, f);
		t.setField(1, new IntField(count));
	    }
	    return t;
	}
	public void rewind() throws DbException, TransactionAbortedException {
	    if (illegal) throw new IllegalStateException();
	    close();
	    open();
	}
	public TupleDesc getTupleDesc() { 
	    return td;
	}
	public void close() { 
	    illegal = true;
	}
    }

}
