package simpledb;

import java.io.IOException;
/**
 * Inserts tuples read from the child operator into the tableid specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    private TransactionId t;
    private DbIterator[] children;
    private int tableid;
    private boolean called;

    /**
     * Constructor.
     * 
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableid
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t,DbIterator child, int tableid)
            throws DbException {
        // some code goes here
	this.t = t;
	this.children = new DbIterator[] {child, null};
	this.tableid = tableid;
	called = false;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return new TupleDesc(new Type[] {Type.INT_TYPE});
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
	children[0].open();
	super.open();
    }

    public void close() {
        // some code goes here
	children[0].close();
	super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
	close();
	open();
    }

    /**
     * Inserts tuples read from child into the tableid specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     * 
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
	if (!called) {
	    int count = 0;
	    BufferPool b = Database.getBufferPool();
	    while(children[0].hasNext()) {
		try {
		    b.insertTuple(t, tableid, children[0].next());
		    count++;
		} catch (IOException e) {
		    System.out.println("Nahhhhh, man");
		    continue;
		}
	    }
	    Tuple t = new Tuple(getTupleDesc());
	    t.setField(0, new IntField(count));
	    called = true;
	    return t;
	} else {
	    return null;
	}
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
