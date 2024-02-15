package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"

	"github.com/go-faker/faker/v4"
	//. "github.com/go-jet/jet/v2/sqlite"
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

type structFakeData struct {
	User       string            `faker:"username"`
	Address    faker.RealAddress `faker:"real_address"`
	Country    string            `faker:"oneof: US CA MX UK AU AT DE"`
	AreaCode   string            `faker:"boundary_start=100, boundary_end=999"`
	ZipCode    string            `faker:"boundary_start=10000, boundary_end=99999"`
	YearBirth  int32             `faker:"boundary_start=1920, boundary_end=2006"`
	Name       string            `faker:"name"`
	CreatedTst string            `faker:"timestamp"`
	ChangedTst string            `faker:"timestamp"`
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

func dbInit() (db *sql.DB, err error) {
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

func dbCreateSchema(opt opts) (err error) {
	const dbDDL = `
		CREATE TABLE IF NOT EXISTS "user" (
			"user" TEXT NOT NULL,
			city TEXT,
			region TEXT,
			country TEXT,
			area_code TEXT,
			zip_code TEXT,
			year_birth INTEGER,
			im TEXT,
			name TEXT,
			created_tst DATETIME NOT NULL DEFAULT (STRFTIME('%F %T','now','localtime')),
			changed_tst DATETIME NOT NULL DEFAULT (STRFTIME('%F %T','now','localtime')),
			CONSTRAINT USER_PK PRIMARY KEY ("user")
		);
		CREATE TRIGGER trg_user_update AFTER UPDATE
			OF user, city, region, country, area_code, zip_code, year_birth, im, name_female, name_male, target, shared, active
			ON user
		BEGIN
		UPDATE user
			SET changed_tst = STRFTIME('%F %T','now','localtime')
		WHERE old.user = new.user;
		END;
	`
	_, err = opt.db.Exec(dbDDL)
	return
}

func dbCleanUp(opt opts) (err error) {
	_, err = opt.db.Exec(`DELETE user;`)
	if err != nil {
		log.Fatal(err)
	}
	_, err = opt.db.Exec(`VACUUM;`)
	if err != nil {
		log.Fatal(err)
	}
	return
}

func genData() (fakeData []models.RawSqlUser, err error) {
	for i := 0; i <= 11000; i++ {
		a := structFakeData{}
		err := faker.FakeData(&a)
		if err != nil {
			fmt.Println(err)
		}
		im := `@` + a.User
		s := models.RawSqlUser{
			User:      a.User,
			City:      &a.Address.City,
			Region:    &a.Address.State,
			Country:   &a.Country,
			AreaCode:  &a.AreaCode,
			ZipCode:   &a.ZipCode,
			YearBirth: &a.YearBirth,
			Im:        &im,
			Name:      &a.Name,
		}
		fakeData = append(fakeData, s)
	}
	return 
}

func withRawSQLUpsert(data []models.RawSqlUser, opt opts) {
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
	for _, rec := range data {

		if *opt.useTransaction {
			_, err := tx.Stmt(upsertUser()).Exec(rec.User, rec.City, rec.Region, rec.Country,
				rec.AreaCode, rec.ZipCode, rec.YearBirth, rec.Im, rec.Name)
			if err != nil {
				log.Fatal(err)
			}
		} else {
			_, err := upsertUser().Exec(rec)
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

/*
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
*/

func main() {
	log.Println("Execution Starting")

	var opt opts
	opt.flipYearBirth = flag.Bool("flipYearBirth", false, "Flip YearBirth generation for Upsert Testing")
	opt.useJet = flag.Bool("useJet", false, "Run using Jet module")
	opt.useTransaction = flag.Bool("useTransaction", false, "Wrap work in transaction")
	flag.Parse()

	var err error
	opt.db, err = dbInit()
	if err != nil {
		log.Fatal(err)
	}

	err = dbCleanUp(opt)
	if err != nil {
		log.Fatal(err)
	}

	data, err := genData()
	if err != nil {
		log.Fatal(err)
	}

	//if *opt.useJet {
	//	withJet(data, opt)
	//} else {
	withRawSQLUpsert(data, opt)
	//}

	log.Println("Exection Completed")
}
