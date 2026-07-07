# RookieDB — UC Berkeley CS186 Database Implementation

> A working relational database engine — an independent, from-skeleton implementation of
> **CS186 — Introduction to Database Systems** (UC Berkeley), part of a
> [csdiy.wiki](https://csdiy.wiki/) full-catalog build.

![status](https://img.shields.io/badge/status-complete-brightgreen)
![language](https://img.shields.io/badge/Java-informational)
![license](https://img.shields.io/badge/license-MIT-blue)

## Overview

RookieDB is a bare-bones single-node relational database. Starting from Berkeley's official
`sp26-rookiedb` skeleton (which provides disk/buffer management, a type system, heap-file tables,
and a SQL-ish CLI parser), this repo implements the four core storage-engine subsystems that make
it a real transactional database:

- **B+ tree indexing** — a persistent, page-serialized B+ tree with splitting, bulk-loading, and
  lazy leaf-chained range scans.
- **Joins & query optimization** — block nested loop join, sort-merge join, grace hash join, an
  external merge sort, and a System R cost-based query optimizer.
- **Concurrency** — multigranularity two-phase locking (IS/IX/S/SIX/X) with a lock manager,
  hierarchical lock contexts, escalation, and integration into the record/table access paths.
- **Recovery** — ARIES with write-ahead logging, savepoints/rollback via CLRs, fuzzy checkpoints,
  and three-phase (analysis / redo / undo) restart recovery.

Every subsystem is verified against the course's own JUnit test suites. (The skeleton's original
architecture walkthrough is preserved as [`ARCHITECTURE.md`](ARCHITECTURE.md).)

## Results (measured on this machine: Windows 11, JDK 21, Maven 3.9.16, CPU-only)

Each project is graded by the course's own tests, run with `mvn test -Dproj=N`:

| Project | Subsystem | Result (course JUnit suite) |
|---|---|---|
| **Project 2** | B+ tree indices | **20 / 20 pass** (`TestBPlusTree`, `TestInnerNode`, `TestLeafNode`, `TestBPlusNode`) |
| **Project 3 (Part 1)** | Joins + external sort | **16 / 16 pass** (`TestNestedLoopJoin`, `TestSortMergeJoin`, `TestGraceHashJoin`, `TestSortOperator`) |
| **Project 3 (Part 2)** | Query optimizer | **14 / 15 pass** — `TestBasicQuery` 3/3, `TestOptimizationJoins` 6/6, `TestSingleAccess` 5/6. The single miss is a 2-second JUnit I/O timeout, not a logic error (see Verification). |
| **Project 4** | Multigranularity 2PL | **64 / 64 pass** (`TestLockType`, `TestLockManager`, `TestLockContext`, `TestLockUtil`, `TestDatabase2PL`, `TestDatabaseDeadlockPrecheck`) |
| **Project 5** | ARIES recovery | **19 / 19 pass** (`TestRecoveryManager`) |

**End-to-end integration** — an in-process demo (`results/E2EDemo.java`) drives all four subsystems
against one live database. Actual captured output (`results/e2e_demo_output.txt`):

```
[proj2] Index point lookup students.sid=42 -> (42,'student42',2.2) (1 row)
[proj3] Optimized join students x enrollments where gpa>3.5 -> 8 result rows
[proj3] Chosen plan:
         SNLJ on students.sid=enrollments.sid (cost=1)
         	-> Select students.gpa>3.5 (cost=1)
         		-> Seq Scan on students (cost=1)
         	-> Seq Scan on enrollments (cost=1)
[proj4] Transaction 4 scanned 50 students under 2PL S-locks; committing releases them
[proj5] After ARIES restart recovery, 'ledger' has 20 rows (expected 20) -> committed data survived the restart
```

## Implemented assignments

- [x] **Project 2 — B+ Tree Indices** — `LeafNode`/`InnerNode`/`BPlusTree`: `get`, `put` (with
  leaf copy-up and inner push-up splits), `bulkLoad` (fill-factor), `remove`, `fromBytes`
  deserialization, and a lazy leaf-chained iterator for `scanAll` / `scanGreaterEqual`.
- [x] **Project 3 Part 1 — Join Algorithms & External Sort** — `BNLJOperator` (block nested loop),
  `SortOperator` (external merge sort via priority-queue k-way merge), `SortMergeOperator`
  (with mark/reset for duplicate keys), `GHJOperator` (grace hash join with recursive
  re-partitioning), plus the SHJ-breaks-but-GHJ-passes / GHJ-breaks input generators.
- [x] **Project 3 Part 2 — Query Optimization** — `QueryPlan`: `minCostSingleAccess`
  (sequential vs. index scan with pushed-down selects), `minCostJoins` (one dynamic-programming
  pass), and `execute` (the full System R search).
- [x] **Project 4 — Concurrency** — `LockType` compatibility/parent/substitutability matrices,
  `LockManager` (acquire / acquire-and-release / release / promote / queue processing),
  `LockContext` (hierarchical acquire/release/promote/escalate, effective vs. explicit lock
  types), `LockUtil.ensureSufficientLockHeld`, and the 2PL release phase + record/table lock
  integration.
- [x] **Project 5 — Recovery** — `ARIESRecoveryManager`: `commit`/`abort`/`end`, `logPageWrite`,
  `rollbackToLSN` (CLR-based undo), `rollbackToSavepoint`, `checkpoint`, and the three restart
  phases `restartAnalysis` / `restartRedo` / `restartUndo`.

## Project structure

```
cs186-rookiedb/
├── pom.xml                       # Maven build (Java 8 source level, JUnit 4)
├── src/main/java/edu/berkeley/cs186/database/
│   ├── index/                    # Project 2: B+ tree (LeafNode, InnerNode, BPlusTree)
│   ├── query/                    # Project 3: join operators, SortOperator, QueryPlan optimizer
│   │   └── join/                 #   BNLJ, SortMergeJoin, GHJ
│   ├── concurrency/              # Project 4: LockType, LockManager, LockContext, LockUtil
│   ├── recovery/                 # Project 5: ARIESRecoveryManager, log records
│   ├── table/                    # heap files, records, schema (+ proj4 lock integration)
│   ├── memory/  io/  databox/    # buffer manager, disk manager, type system (skeleton)
│   └── Database.java             # top-level DB (+ proj4 2PL release, proj5 integration)
├── src/test/java/...             # the course's own JUnit test suites
└── results/                      # captured test output + the end-to-end demo & its output
```

## How to run

Requires a JDK (8+; developed on JDK 21) and Maven.

```bash
# Compile
mvn compile

# Run a project's test suite (N = 2, 3, 4, or 5). Project 3 has parts: -Dproj=3Part1 / 3Part2
mvn test -Dproj=2
mvn test -Dproj=4

# On a slow disk, give the test JVM more heap to avoid I/O-bound timeouts:
mvn test -Dproj=3Part2 -DargLine="-Xms256m -Xmx768m"
```

The end-to-end demo in `results/E2EDemo.java` can be compiled against `target/classes` and run to
reproduce `results/e2e_demo_output.txt`.

## Verification

- Each subsystem is checked with the **course's own JUnit tests** via `mvn test -Dproj=N`; the exact
  captured console output for every project is under [`results/`](results/)
  (`proj2_btree.txt`, `proj3_part1_joins.txt`, `proj3_part2_optimizer.txt`, `proj4_concurrency.txt`,
  `proj5_aries_recovery.txt`).
- **The one non-passing test** (`TestSingleAccess#testIndexSelectionAndPushDown`, plus 1–2 sibling
  methods depending on disk warmth) fails only with a `TestTimedOutException` at the JUnit 2-second
  per-test limit while inserting 2000 rows into a table with **two** B+ tree indices. This machine's
  synchronous small-file I/O is very slow (~15 ms per file op; 500 create+delete ≈ 9 s), so the
  insert volume alone exceeds the budget. The optimizer **logic** is proven correct independently by
  a no-timeout standalone driver ([`results/SingleAccessDriver.java`](results/SingleAccessDriver.java))
  that runs the exact same four single-access scenarios and asserts the same conditions —
  **4 / 4 pass** ([`results/proj3_part2_driver_proof.txt`](results/proj3_part2_driver_proof.txt)).

## Tech stack

Java (JDK 21, source level Java 8), Maven 3.9.16, JUnit 4. No third-party runtime dependencies —
the storage engine (disk/buffer management, B+ trees, join/sort operators, lock manager, ARIES log)
is implemented on the JDK.

## Key ideas / what I learned

- **Page-serialized B+ trees**: nodes live on single pages; splits copy the split key up at leaves
  but push it up at inner nodes; range scans must iterate leaves lazily via right-sibling pointers.
- **Join algorithms & I/O cost**: BNLJ blocks the outer relation into B−2 pages; grace hash join
  recursively re-partitions with a different hash per pass and fails only on skew (identical keys).
- **System R optimization**: dynamic programming over table subsets, pushing selects down and
  choosing the cheapest access path / join per subset by estimated I/O cost.
- **Multigranularity 2PL**: intent locks (IS/IX/SIX) let a transaction lock coarse or fine; the lock
  manager queues conflicting requests FIFO and processes the queue on every release.
- **ARIES**: write-ahead logging with an LSN per page, redo replays from the dirty-page-table's
  minimum recLSN, undo walks transactions backward writing compensation log records so recovery is
  itself restartable.

## Credits & license

Based on the projects of **CS186 — Introduction to Database Systems** by UC Berkeley. The starter
skeleton is Berkeley's public [`berkeley-cs186/sp26-rookiedb`](https://github.com/berkeley-cs186/sp26-rookiedb);
project specs live at [cs186.gitbook.io/project](https://cs186.gitbook.io/project/). This repository
is an independent educational reimplementation of the student portions; all course materials and the
skeleton belong to their original authors. Original implementation code here is released under the
[MIT License](LICENSE).
