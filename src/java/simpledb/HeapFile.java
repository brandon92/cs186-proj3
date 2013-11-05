package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

  private File file;
  private TupleDesc td;

  /**
   * Constructs a heap file backed by the specified file.
   * 
   * @param f
   *            the file that stores the on-disk backing store for this heap
   *            file.
   */
  public HeapFile(File f, TupleDesc td) {
    // some code goes here
    this.file = f;
    this.td = td;
  }

  /**
   * Returns the File backing this HeapFile on disk.
   * 
   * @return the File backing this HeapFile on disk.
   */
  public File getFile() {
    // some code goes here
    return file;
  }

  /**
   * Returns an ID uniquely identifying this HeapFile. Implementation note:
   * you will need to generate this tableid somewhere ensure that each
   * HeapFile has a "unique id," and that you always return the same value for
   * a particular HeapFile. We suggest hashing the absolute file name of the
   * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
   * 
   * @return an ID uniquely identifying this HeapFile.
   */
  public int getId() {
    // some code goes here
    return file.getAbsoluteFile().hashCode();
  }

  /**
   * Returns the TupleDesc of the table stored in this DbFile.
   * 
   * @return TupleDesc of this DbFile.
   */
  public TupleDesc getTupleDesc() {
    // some code goes here
    return td;
  }

  // see DbFile.java for javadocs
  public Page readPage(PageId pid) {
    // some code goes here

    if(pid.getTableId() != getId()) {
	throw new IllegalArgumentException("Page not in file");
    }

    try {
	int pageNum = pid.pageNumber();
	byte[] data = new byte[BufferPool.PAGE_SIZE];
	
	RandomAccessFile raf = new RandomAccessFile(file, "r");
	raf.seek(pageNum * BufferPool.PAGE_SIZE);
	raf.read(data, 0,  BufferPool.PAGE_SIZE);
	raf.close();
	return (new HeapPage((HeapPageId)pid, data));
    } catch (FileNotFoundException e1) {
	System.err.println("FileNotFoundException");
	e1.printStackTrace();
    } catch (IOException e) {
	System.err.println("IOException");
	e.printStackTrace();
    }
    // Should not reach here
    throw new IllegalArgumentException();
  }

  // see DbFile.java for javadocs
  public void writePage(Page page) throws IOException {
    // some code goes here
    // not necessary for proj1
    int offset = page.getId().pageNumber();
    byte[] data = page.getPageData();
    RandomAccessFile raf = new RandomAccessFile(file, "rw");
    raf.seek(offset * BufferPool.PAGE_SIZE);
    raf.write(data);
    raf.close();
  }

  /**
   * Returns the number of pages in this HeapFile.
   */
  public int numPages() {
    // some code goes here
    double pageSize = (double)BufferPool.PAGE_SIZE;
    return (int)Math.floor((double)file.length() / pageSize);
  }

  // see DbFile.java for javadocs
  public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
    throws DbException, IOException, TransactionAbortedException {
    // some code goes here
    ArrayList<Page> results = new ArrayList<Page>();
    for(int i = 0; i < numPages(); i++) {
	HeapPageId pid = new HeapPageId(getId(), i);
	HeapPage p = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
	try {
	    p.insertTuple(t);
	    // Adds to result/returns iff there is a page with an empty slot
	    results.add(p);
	    return results;
	} catch (DbException e) {
	    continue;
	}
    }
    // No pages left, add new page
    HeapPageId pid = new HeapPageId(getId(), numPages());
    HeapPage p = new HeapPage(pid, HeapPage.createEmptyPageData());
    p.insertTuple(t);
    writePage(p);
    results.add(p);
    return results;
    
  }

  // see DbFile.java for javadocs
  public Page deleteTuple(TransactionId tid, Tuple t) throws DbException,
    TransactionAbortedException {
    // some code goes here
    RecordId rid = t.getRecordId();
    PageId pid = rid.getPageId();
    HeapPage p = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
    p.deleteTuple(t);
    return p;
    // not necessary for proj1
  }

  // see DbFile.java for javadocs
  public DbFileIterator iterator(TransactionId tid) {
    // some code goes here
    return (new MyIter(tid));
  }

  private class MyIter implements DbFileIterator {
    Iterator<Tuple> tupleIter;
    int currentPageNum;
    TransactionId tid;

    public MyIter(TransactionId tid) {
      this.tid = tid;
    }

    public void open()
      throws DbException, TransactionAbortedException {
      currentPageNum = 0;
      setIterPage(currentPageNum);
    }

    public boolean hasNext()
      throws DbException, TransactionAbortedException {
      if (tupleIter == null) {
        return false;
      } else if (tupleIter.hasNext()) {
	return true;
      } else if (currentPageNum + 1 < numPages()) {
	currentPageNum++;
	setIterPage(currentPageNum);
	return hasNext();
      } else {
	return false;
      }
    }

    public Tuple next()
      throws DbException, TransactionAbortedException, NoSuchElementException {
      // either hasn't been opened yet, or has been closed
      if (tupleIter == null) {
        throw (new NoSuchElementException());
      }
      if (tupleIter.hasNext()) {
        return tupleIter.next();
      } else {
        throw (new NoSuchElementException());
      }
    }
    public void rewind() throws DbException, TransactionAbortedException {
      close();
      open();
    }

    public void close() {
      tupleIter = null;
    }

    // Encapsulates using the next page in tupleIter
    private void setIterPage(int pageNum) throws DbException, TransactionAbortedException{
      HeapPageId pid = new HeapPageId(getId(), pageNum);
      Page page = Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
      tupleIter = ((HeapPage) page).iterator();
      
    }
  }








}

