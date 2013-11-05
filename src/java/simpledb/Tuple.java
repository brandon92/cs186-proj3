package simpledb;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;
import java.lang.StringBuilder;

/**
 * Tuple maintains information about the contents of a tuple. Tuples have a
 * specified schema specified by a TupleDesc object and contain Field objects
 * with the data for each field.
 */
public class Tuple implements Serializable {

  private static final long serialVersionUID = 1L;

  private TupleDesc _td;
  private RecordId _rid;
  private Field[] _fields;
  private int joinIndex;

  /**
   * Create a new tuple with the specified schema (type).
   * 
   * @param td
   *            the schema of this tuple. It must be a valid TupleDesc
   *            instance with at least one field.
   */
  public Tuple(TupleDesc td) {
    // some code goes here
    _td = td;
    if (td.numFields() < 1) {
      throw (new UnsupportedOperationException("Tuple has no fields"));
    }
    _fields = new Field[td.numFields()];
    _rid = null;
    joinIndex = -1;

  }

  /**
   * @return The TupleDesc representing the schema of this tuple.
   */
  public TupleDesc getTupleDesc() {
    // some code goes here
    return _td;
  }

  /**
   * @return The RecordId representing the location of this tuple on disk. May
   *         be null.
   */
  public RecordId getRecordId() {
    // some code goes here
    return _rid;
  }

  /**
   * Set the RecordId information for this tuple.
   * 
   * @param rid
   *            the new RecordId for this tuple.
   */
  public void setRecordId(RecordId rid) {
    // some code goes here
    _rid = rid;
  }

  /**
   * Change the value of the ith field of this tuple.
   * 
   * @param i
   *            index of the field to change. It must be a valid index.
   * @param f
   *            new value for the field.
   */
  public void setField(int i, Field f) {
    // some code goes here
    if(f.getType() != _td.getFieldType(i)) throw new RuntimeException("type mismatch");
    _fields[i] = f;
  }

  public static Tuple merge(Tuple t1, Tuple t2) {
      TupleDesc td3 = TupleDesc.merge(t1.getTupleDesc(), t2.getTupleDesc());
      Tuple t3 = new Tuple(td3);
      int i = 0;
      Iterator<Field> f1 = t1.fields();
      Iterator<Field> f2 = t2.fields();
      while (f1.hasNext()) {
	  t3.setField(i, f1.next());
	  i++;
      }
      while (f2.hasNext()) {
	  t3.setField(i, f2.next());
	  i++;
      }
      return t3;
  }

  /**
   * @return the value of the ith field, or null if it has not been set.
   * 
   * @param i
   *            field index to return. Must be a valid index.
   */
  public Field getField(int i) {
    // some code goes here
    return _fields[i];
  }

  /**
   * Returns the contents of this Tuple as a string. Note that to pass the
   * system tests, the format needs to be as follows:
   * 
   * column1\tcolumn2\tcolumn3\t...\tcolumnN\n
   * 
   * where \t is any whitespace, except newline, and \n is a newline
   */
  public String toString() {
    // some code goes here
    //throw new UnsupportedOperationException("Implement this");
    String result="";
    for (int i = 0; i < _fields.length-1; i++) {
	result+= _fields[i].toString() + "\t";
    }
    result += _fields[_fields.length-1].toString() + "\n";
    return result;
  }

  /**
   * @return
   *        An iterator which iterates over all the fields of this tuple
   * */
  public Iterator<Field> fields()  {
    // some code goes here
      return Arrays.asList(_fields).iterator();
  }

  @Override
  public boolean equals(Object o) {
      if (o instanceof Tuple) {
	  Tuple t = (Tuple) o;
	  if (joinIndex == -1) {
	      return this.toString().equals(t.toString());
	  } else {
	      return joinField().equals(t.joinField());
		  }
      } else {
	  return false;
      }
  }

  public Field joinField() {
      return getField(joinIndex);
  }

  public void setJoinIndex(int index) {
      joinIndex = index;
  }

  @Override
  public int hashCode() {
      return joinField().hashCode();
  }
}
