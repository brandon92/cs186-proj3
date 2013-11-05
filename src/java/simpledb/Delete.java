package simpledb;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;

    private TransactionId t;
    private DbIterator[] children;
    private boolean called;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, DbIterator child) {
        // some code goes here
	this.t = t;
	this.children = new DbIterator[] {child, null};
	this.called = false;
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
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
	int count;
	if(!called) {
	    count = 0;
	    BufferPool b = Database.getBufferPool();
	    while(children[0].hasNext()) {
		b.deleteTuple(t, children[0].next());
		count++;
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
