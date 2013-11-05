package simpledb;

class Table {

  private String pkeyField;
  private DbFile file;
  public Table(DbFile f, String k) {
    file = f;
    pkeyField = k;
  }
  public String getKey() {
    return pkeyField;
  }

  public DbFile getFile() {
    return file;
  }
    
}
