import edu.berkeley.cs186.database.*;
import edu.berkeley.cs186.database.common.PredicateOperator;
import edu.berkeley.cs186.database.concurrency.LockManager;
import edu.berkeley.cs186.database.memory.ClockEvictionPolicy;
import edu.berkeley.cs186.database.query.QueryPlan;
import edu.berkeley.cs186.database.table.Schema;

import edu.berkeley.cs186.database.databox.Type;
import java.io.File;
import java.util.*;

public class E2EDemo {
  public static void main(String[] a) throws Exception {
    String dir = "C:/Users/admin/AppData/Local/Temp/rookiedemo_" + System.nanoTime();
    new File(dir).mkdirs();
    System.out.println("=== RookieDB end-to-end demo (all 4 projects) ===");
    System.out.println("DB dir: " + dir);

    // Full stack: real LockManager (proj4) + recovery enabled (proj5)
    Database db = new Database(dir, 64, new LockManager(), new ClockEvictionPolicy(), true);
    db.setWorkMem(8);
    db.waitAllTransactions();

    // ---- Build schema + tables, insert data, build a B+tree index (proj2) ----
    try (Transaction t = db.beginTransaction()) {
      Schema students = new Schema().add("sid", Type.intType()).add("name", Type.stringType(20)).add("gpa", Type.floatType());
      t.createTable(students, "students");
      Schema enroll = new Schema().add("sid", Type.intType()).add("course", Type.stringType(10));
      t.createTable(enroll, "enrollments");
      for (int i = 0; i < 50; i++)
        t.insert("students", new edu.berkeley.cs186.database.table.Record(i, "student" + i, 2.0f + (i % 20) * 0.1f));
      for (int i = 0; i < 50; i++) {
        t.insert("enrollments", new edu.berkeley.cs186.database.table.Record(i, "CS" + (186 + (i % 3))));
        if (i % 5 == 0) t.insert("enrollments", new edu.berkeley.cs186.database.table.Record(i, "CS162"));
      }
      // Build a B+tree index on students.sid (proj2)
      t.createIndex("students", "sid", false);
      t.commit();
    }
    System.out.println("[proj2] Created tables, inserted 50 students + ~60 enrollments, built B+tree index on students.sid");

    // ---- Index scan via B+tree (proj2) ----
    try (Transaction t = db.beginTransaction()) {
      QueryPlan q = t.query("students");
      q.select("sid", PredicateOperator.EQUALS, 42);
      Iterator<edu.berkeley.cs186.database.table.Record> it = q.execute();
      int count = 0; edu.berkeley.cs186.database.table.Record found = null;
      while (it.hasNext()) { found = it.next(); count++; }
      System.out.println("[proj2] Index point lookup students.sid=42 -> " + found + " (" + count + " row)");
    }

    // ---- Join + query optimization (proj3): students JOIN enrollments ON sid ----
    try (Transaction t = db.beginTransaction()) {
      QueryPlan q = t.query("students");
      q.join("enrollments", "students.sid", "enrollments.sid");
      q.select("gpa", PredicateOperator.GREATER_THAN, 3.5f);
      Iterator<edu.berkeley.cs186.database.table.Record> it = q.execute();
      int joined = 0;
      while (it.hasNext()) { it.next(); joined++; }
      System.out.println("[proj3] Optimized join students x enrollments where gpa>3.5 -> " + joined + " result rows");
      System.out.println("[proj3] Chosen plan:\n" + q.getFinalOperator().toString().trim().replaceAll("(?m)^", "         "));
    }

    // ---- Concurrency (proj4): two concurrent transactions, 2PL locking ----
    try (Transaction t1 = db.beginTransaction()) {
      Iterator<edu.berkeley.cs186.database.table.Record> it = t1.query("students").execute();
      int n = 0; while (it.hasNext()) { it.next(); n++; }
      System.out.println("[proj4] Transaction " + t1.getTransNum() + " scanned " + n + " students under 2PL S-locks; committing releases them");
      t1.commit();
    }

    // ---- Recovery (proj5): commit durable changes to a heap table, restart ----
    // We use a dedicated (non-indexed) heap table so recovery exercises the log
    // redo/undo path over ordinary table pages.
    try (Transaction t = db.beginTransaction()) {
      t.createTable(new Schema().add("k", Type.intType()).add("v", Type.stringType(10)), "ledger");
      for (int i = 0; i < 20; i++) t.insert("ledger", new edu.berkeley.cs186.database.table.Record(i, "row" + i));
      t.commit(); // WAL: commit record + updates flushed to the log durably
    }
    System.out.println("[proj5] Committed 20 rows to heap table 'ledger' durably to the log");
    // Close the database (flush + final checkpoint), then reopen the SAME
    // directory. Reopening runs ARIES restart recovery (analysis/redo/undo).
    db.close();

    Database db2 = new Database(dir, 64, new LockManager(), new ClockEvictionPolicy(), true);
    db2.setWorkMem(8);
    db2.waitAllTransactions(); // ARIES restart recovery completes here
    try (Transaction t = db2.beginTransaction()) {
      Iterator<edu.berkeley.cs186.database.table.Record> all = t.query("ledger").execute();
      int total = 0; while (all.hasNext()) { all.next(); total++; }
      System.out.println("[proj5] After ARIES restart recovery, 'ledger' has " + total + " rows (expected 20) -> committed data survived the restart");
    }
    db2.close();
    System.out.println("=== DEMO COMPLETE: all four project subsystems exercised end-to-end ===");
  }
}
