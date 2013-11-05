package simpledb;

import java.util.*;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;
    
    private DbIterator[] children;
    private int afield;
    private int gfield;
    private Aggregator.Op aop;
    private DbIterator dbIter;

    /**
     * Constructor.
     * 
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     * 
     * 
     * @param child
     *            The DbIterator that is feeding us tuples.
     * @param afield
     *            The column over which we are computing an aggregate.
     * @param gfield
     *            The column over which we are grouping the result, or -1 if
     *            there is no grouping
     * @param aop
     *            The aggregation operator to use
     */
    public Aggregate(DbIterator child, int afield, int gfield, Aggregator.Op aop) {
	// some code goes here
	this.children = new DbIterator[] {child, null};
	this.afield = afield;
	this.gfield = gfield;
	this.aop = aop;
	Type atype = child.getTupleDesc().getFieldType(afield);
	Type gbfieldtype = ((gfield == -1) ? null : child.getTupleDesc().getFieldType(gfield));
	Aggregator ag;
	if (atype == Type.STRING_TYPE) {
	    ag = new StringAggregator(gfield, gbfieldtype, afield, aop);
	} else if (atype == Type.INT_TYPE) {
	    ag = new IntegerAggregator(gfield, gbfieldtype, afield, aop);
	} else {
	    ag = null;
	}

	try {
	    child.open();
	    while (child.hasNext()) {
		ag.mergeTupleIntoGroup(child.next());
	    }
	} catch (DbException e) {
	    e.printStackTrace();
	} catch (TransactionAbortedException e) {
	    e.printStackTrace();
	}
	child.close();
	dbIter = ag.iterator();
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     *         field index in the <b>INPUT</b> tuples. If not, return
     *         {@link simpledb.Aggregator#NO_GROUPING}
     * */
    public int groupField() {
	// some code goes here
	return gfield;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     *         of the groupby field in the <b>OUTPUT</b> tuples If not, return
     *         null;
     * */
    public String groupFieldName() {
	// some code goes here
	return children[0].getTupleDesc().getFieldName(gfield);
    }

    /**
     * @return the aggregate field
     * */
    public int aggregateField() {
	// some code goes here
	return afield;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     *         tuples
     * */
    public String aggregateFieldName() {
	// some code goes here
	return children[0].getTupleDesc().getFieldName(afield);
    }

    /**
     * @return return the aggregate operator
     * */
    public Aggregator.Op aggregateOp() {
	// some code goes here
	return aop;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
	return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
	    TransactionAbortedException {
	// some code goes here
	dbIter.open();
	super.open();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate, If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
	// some code goes here
	while (dbIter.hasNext()) {
	    return dbIter.next();
	}
	return null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
	// some code goes here
	close();
	open();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * 
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
	// some code goes here
	return dbIter.getTupleDesc();
    }

    public void close() {
	// some code goes here
	dbIter.close();
	super.close();
    }

    @Override
    public DbIterator[] getChildren() {
	// some code goes here
	return children;
    }

    @Override
    public void setChildren(DbIterator[] children) {
	// some code goes here
	this.children = children;
    }
    
}