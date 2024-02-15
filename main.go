package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"time"

	. "github.com/go-jet/jet/v2/sqlite"
	_ "github.com/mattn/go-sqlite3"
	"github.com/schollz/progressbar/v3"

	"github.com/lbe/go-sql-test/models"
)

type opts struct {
	db             *sql.DB
	flipYearBirth  *bool
	useJet         *bool
	useTransaction *bool
}

type structData struct {
	fakeData []models.RawSqlUser
}

func getPtrNullableString(str string) *string {
	if str == "" {
		return nil
	} else {
		return &str
	}
}

func getPtrNullableInt(i int32) *int32 {
	if i == 0 {
		return nil
	} else {
		return &i
	}
}

func db_init() (db *sql.DB, err error) {
	dbFileName := "./data/wl.sqlite"
	log.Printf("dbFilename = %s\n", dbFileName)

	dsn := dbFileName
	dsn += "?cache=shared&_journal_mode=WAL"
	dsn += "&_synchronous=NORMAL" // OFF added for testing
	log.Printf("dsn = %s", dsn)

	db, err = sql.Open("sqlite3", dsn)
	if err != nil {
		log.Println("Error:", err)
		return
	}
	if err := db.Ping(); err != nil {
		log.Fatal(err)
	}

	return
}

func genData() (fakeData structData, err error) {

	return
}

func withRawSQLUpsert(data structData, opt opts) {
	log.Println("Executing withRawSQLUpsert")

	var tx *sql.Tx
	if *opt.useTransaction {
		// Get a Tx for making transaction requests.
		var err error
		tx, err = opt.db.Begin()
		if err != nil {
			log.Fatal(err)
		}
		// Defer a rollback in case anything fails.
		defer tx.Rollback()
	}

	upsertUser := models.StmtUpsertUser(opt.db)
	bar := progressbar.Default(int64(len(data)))
	for _, row := range data {
		var rec models.RawSqlUser
		
		if *opt.useTransaction {
			_, err = tx.Stmt(upsertUser()).Exec(rec)
			if err != nil {
				log.Fatal(err)
		}
		} else {
			_, err = upsertUser().Exec(rec)
			if err != nil {
				log.Fatal(err)
			}
		}
		bar.Add(1)
	}
	bar.Finish()

	if *opt.useTransaction {
		log.Print("Commit Start")
		tx.Commit()
		log.Print("Commit Finished")
	}
}

func withJet(data structData, opt opts) {
	log.Println("Executing withJet")

	var tx *sql.Tx
	if *opt.useTransaction {
		// Get a Tx for making transaction requests.
		var err error
		tx, err = opt.db.Begin()
		if err != nil {
			log.Fatal(err)
		}
		// Defer a rollback in case anything fails.
		defer tx.Rollback()
	}

	bar := progressbar.Default(int64(len(data.Rows)))
	for _, row := range data.Rows {
		var rec model.User
		
		columnList := ColumnList{
			User.User, User.City, User.Region, User.Country, User.AreaCode, User.ZipCode,
			User.YearBirth, User.Im, User.Name, 		
		}

		stmtInserUser := User.INSERT(columnList).
			MODEL(rec).
			ON_CONFLICT(User.User).
			DO_UPDATE(
				SET(
					User.City.SET(User.EXCLUDED.City),
					User.Country.SET(User.EXCLUDED.Country),
					User.AreaCode.SET(User.EXCLUDED.AreaCode),
					User.ZipCode.SET(User.EXCLUDED.ZipCode),
					User.YearBirth.SET(User.EXCLUDED.YearBirth),
					User.Im.SET(User.EXCLUDED.Im),
					User.Name.SET(User.EXCLUDED.Name),
				).WHERE(
					OR(User.Country.IS_DISTINCT_FROM(User.EXCLUDED.Country)).
						OR(User.AreaCode.IS_DISTINCT_FROM(User.EXCLUDED.AreaCode)).
						OR(User.ZipCode.IS_DISTINCT_FROM(User.EXCLUDED.ZipCode)).
						OR(User.YearBirth.IS_DISTINCT_FROM(User.EXCLUDED.YearBirth)).
						OR(User.Im.IS_DISTINCT_FROM(User.EXCLUDED.Im)).
						OR(User.Name.IS_DISTINCT_FROM(User.EXCLUDED.Name))
				),
			)

		if *opt.useTransaction {
			_, err = stmtInserUser.Exec(tx)
			if err != nil {
				fmt.Println(err, row)
			}
		} else {
			_, err = stmtInserUser.Exec(opt.db)
			if err != nil {
				fmt.Println(err, row)
			}
		}
		bar.Add(1)
	}
	bar.Finish()

	if *opt.useTransaction {
		log.Print("Commit Start")
		tx.Commit()
		log.Print("Commit Finished")
	}

	return
}

func main() {
	log.Println("Execution Starting")

	var opt opts
	opt.flipYearBirth = flag.Bool("flipYearBirth", false, "Flip YearBirth generation for Upsert Testing")
	opt.useJet = flag.Bool("useJet", false, "Run using Jet module")
	opt.useTransaction = flag.Bool("useTransaction", false, "Wrap work in transaction")
	flag.Parse()
	
	db, err := db_init()
	if err != nil {
		log.Fatal(err)
	}

	if *opt.useJet {
		withJet(data, opt)
	} else {
		withRawSQLUpsert(data, opt)
	}

	log.Println("Exection Completed")
}
