# go-sql-test
This is a project to test Go SQL database performance with and without 
[prepared statements](https://go.dev/doc/database/prepared-statements)
using both raw SQL and the Jet package.  Both of these approaches depend upon the Go standard
library [database/sql](https://pkg.go.dev/database/sql) and the 
[mattn/go-sqlite3](https://pkg.go.dev/github.com/mattn/go-sqlite3) driver.

The motivation behind creating this project is to support a pull request that I made to the 
Jet project to support prepared statements.  I am working on a project similar to those that
I have written in other languages where I have received performance benefits from using prepared 
statements with the largest benefit coming from mass insertions.

This code will execute three scenarios: Insert, Update and Select for RawSQL and/or Jet based upon
the command line instructions.

The command line options are shown with the -h flag
```console
Usage of ./go-sql-test:
  -cpuprofile string
    	write cpu profile to file
  -rowCount int
    	Number of rows to use in test (default 10000)
  -updateCount int
    	Maximum number of updates to perform (default 1000)
  -useBoth
    	Run both RawSql and Jet
  -useJet
    	Run using Jet module
  -useRawSQL
    	Run using RawSQL module
  -useTransaction
    	Wrap work in transaction
```


The following show the command to run both scenarios inside a transaction with the default row and update count.
```console
[16:40:16] ➜  ./go-sql-test -useBoth -useTransaction
2024/02/17 16:40:17 Execution Starting
2024/02/17 16:40:17 dbFilename = ./data/go-sql-test.sqlite
2024/02/17 16:40:17 dsn = ./data/go-sql-test.sqlite?cache=shared&_journal_mode=WAL&_synchronous=NORMAL
2024/02/17 16:40:18 Sort data Starting
2024/02/17 16:40:18 Sort data Ended
2024/02/17 16:40:18 Executing insertWithRawSQLUpsert
 100% |███████████████████████████████████████████████████████████████████████████████████████████| (10000/10000, 100602 it/s)
2024/02/17 16:40:18 Commit Start
2024/02/17 16:40:18 Commit Finished
2024/02/17 16:40:18 Executing updateWithRawSQLUpsert
 100% |█████████████████████████████████████████████████████████████████████████████████████████████████| (999/1000, 76 it/s)
2024/02/17 16:40:31 Commit Start
2024/02/17 16:40:31 Commit Finished
2024/02/17 16:40:31 Executing selectWithRawSQLUpsert
 100% |████████████████████████████████████████████████████████████████████████████████████████████| (10000/10000, 46476 it/s)
2024/02/17 16:40:31 Commit Start
2024/02/17 16:40:31 Commit Finished
2024/02/17 16:40:31 Reset database for Jet
2024/02/17 16:40:31 Executing insertWithJet
 100% |████████████████████████████████████████████████████████████████████████████████████████████| (10000/10000, 11311 it/s)
2024/02/17 16:40:32 Commit Start
2024/02/17 16:40:32 Commit Finished
2024/02/17 16:40:32 Executing updateWithJet
 100% |█████████████████████████████████████████████████████████████████████████████████████████████████| (999/1000, 74 it/s)
2024/02/17 16:40:45 Commit Start
2024/02/17 16:40:45 Commit Finished
2024/02/17 16:40:45 Executing selectWithJet
 100% |████████████████████████████████████████████████████████████████████████████████████████████| (10000/10000, 37608 it/s)
2024/02/17 16:40:46 Commit Start
2024/02/17 16:40:46 Commit Finished
2024/02/17 16:40:46 Execution Completed

The insert rate with the RawSQL prepared statement (100,602 per sec) is almost 10 times that of the non-prepared Jet case (11,311 per second).

The benefit on select is there but is on only about a 20%.

The current update rate is abysmal.  I suspect that I am not doing something very optimized with it.

This is the initial shaing of this code on Feb 17, 2024.  I suspect that it will be revised more than a few times.

lbe 
