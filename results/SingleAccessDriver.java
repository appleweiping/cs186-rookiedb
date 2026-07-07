import edu.berkeley.cs186.database.*;
import edu.berkeley.cs186.database.common.PredicateOperator;
import edu.berkeley.cs186.database.query.*;
import edu.berkeley.cs186.database.table.Schema;
import edu.berkeley.cs186.database.TestUtils;
import java.io.File;

public class SingleAccessDriver {
  static int pass=0, fail=0;
  static void check(String name, boolean cond){ System.out.println(name+": "+(cond?"PASS":"FAIL")); if(cond)pass++; else fail++; }
  public static void main(String[] a) throws Exception {
    String dir = "C:/Users/admin/AppData/Local/Temp/rookiedrv_db_" + System.nanoTime();
    new File(dir).mkdirs();
    Database db = new Database(dir, 32);
    db.setWorkMem(5);
    try(Transaction t = db.beginTransaction()) {
      Schema schema = TestUtils.createSchemaWithAllTypes();
      t.createTable(schema, "table");
      t.createTable(schema, "indexed_table");
      t.createIndex("indexed_table", "int", false);
      t.createTable(schema, "multi_indexed_table");
      t.createIndex("multi_indexed_table", "int", false);
      t.createIndex("multi_indexed_table", "float", false);
    }
    db.waitAllTransactions();

    // testSequentialScanSelection
    try(Transaction tr = db.beginTransaction()) {
      for(int i=0;i<2000;i++) tr.insert("table", new edu.berkeley.cs186.database.table.Record(false,i,"!",0.0f));
      tr.getTransactionContext().getTable("table").buildStatistics(10);
      QueryOperator op = tr.query("table","t1").minCostSingleAccess("t1");
      check("testSequentialScanSelection(isSequentialScan)", op.isSequentialScan());
    }
    // testSimpleIndexScanSelection
    try(Transaction tr = db.beginTransaction()) {
      for(int i=0;i<2000;i++) tr.insert("indexed_table", new edu.berkeley.cs186.database.table.Record(false,i,"!",0.0f));
      tr.getTransactionContext().getTable("indexed_table").buildStatistics(10);
      QueryPlan q = tr.query("indexed_table");
      q.select("int", PredicateOperator.EQUALS, 9);
      QueryOperator op = q.minCostSingleAccess("indexed_table");
      check("testSimpleIndexScanSelection(isIndexScan)", op.isIndexScan());
    }
    // testNoValidIndices
    try(Transaction tr = db.beginTransaction()) {
      for(int i=0;i<2000;i++) tr.insert("multi_indexed_table", new edu.berkeley.cs186.database.table.Record(false,i,"!",(float)i));
      tr.getTransactionContext().getTable("multi_indexed_table").buildStatistics(10);
      QueryOperator op = tr.query("multi_indexed_table").minCostSingleAccess("multi_indexed_table");
      check("testNoValidIndices(isSequentialScan)", op.isSequentialScan());
    }
    // recreate multi_indexed_table fresh for next scenario
    try(Transaction t = db.beginTransaction()) {
      t.dropTable("multi_indexed_table");
      Schema schema = TestUtils.createSchemaWithAllTypes();
      t.createTable(schema, "multi_indexed_table");
      t.createIndex("multi_indexed_table", "int", false);
      t.createIndex("multi_indexed_table", "float", false);
    }
    db.waitAllTransactions();
    // testIndexSelectionAndPushDown
    try(Transaction tr = db.beginTransaction()) {
      for(int i=0;i<2000;i++) tr.insert("multi_indexed_table", new edu.berkeley.cs186.database.table.Record(false,i,"!",(float)i));
      tr.getTransactionContext().getTable("multi_indexed_table").buildStatistics(10);
      QueryPlan q = tr.query("multi_indexed_table");
      q.select("int", PredicateOperator.EQUALS, 9);
      q.select("bool", PredicateOperator.EQUALS, false);
      QueryOperator op = q.minCostSingleAccess("multi_indexed_table");
      check("testIndexSelectionAndPushDown(select over indexscan)", op.isSelect() && op.getSource().isIndexScan());
    }
    db.close();
    System.out.println("SUMMARY: pass="+pass+" fail="+fail);
  }
}
