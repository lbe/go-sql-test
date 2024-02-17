package main

import (
	"database/sql"
	"flag"
	"log"
	"os"
	"runtime"
	"runtime/pprof"
	"sort"
	"strconv"
	//"strings"

	"github.com/go-faker/faker/v4"
	. "github.com/go-jet/jet/v2/sqlite"
	_ "github.com/mattn/go-sqlite3"
	"github.com/schollz/progressbar/v3"

	"github.com/lbe/go-sql-test/gen/model"
	. "github.com/lbe/go-sql-test/gen/table"
	"github.com/lbe/go-sql-test/models"
)

type opts struct {
	db             *sql.DB
	flipYearBirth  *bool
	rowCount       *int
	updateCount    *int
	useBoth        *bool
	useJet         *bool
	useRawSQL      *bool
	useTransaction *bool
}

type structFakeData struct {
	User      string            `faker:"username"`
	Address   faker.RealAddress `faker:"real_address"`
	Country   string            `faker:"oneof: US, CA, MX, UK, AU, AT, DE"`
	AreaCode  int32             `faker:"boundary_start=100, boundary_end=999"`
	ZipCode   int32             `faker:"boundary_start=10000, boundary_end=99999"`
	YearBirth int32             `faker:"boundary_start=1920, boundary_end=2006"`
	Name      string            `faker:"name"`
}

var opt opts

/*
func getPtrNullableString(str string) *string {
	str = strings.TrimSpace(str)
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
*/

func getPtrNullableStringFromInt(i int32) *string {
	str := strconv.FormatInt(int64(i), 10)
	return &str
}

func dbCleanUp() (err error) {
	_, err = opt.db.Exec(`DELETE FROM user;`)
	if err != nil {
		_, filename, line, _ := runtime.Caller(1)
		log.Fatalf("[error] %s:%d %v", filename, line, err)
	}
	_, err = opt.db.Exec(`VACUUM;`)
	if err != nil {
		_, filename, line, _ := runtime.Caller(1)
		log.Fatalf("[error] %s:%d %v", filename, line, err)
	}
	return
}

func dbCreateSchema() (err error) {
	const dbDDL = `
	CREATE TABLE IF NOT EXISTS user (
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
		CREATE TRIGGER IF NOT EXISTS trg_user_update AFTER UPDATE
			OF user, city, region, country, area_code, zip_code, year_birth, im, name_female, name_male, target, shared, active
			ON user
		BEGIN
		UPDATE user
			SET changed_tst = STRFTIME('%F %T','now','localtime')
		WHERE old.user = new.user;
		END;
	`
	_, err = opt.db.Exec(dbDDL)
	if err != nil {
		_, filename, line, _ := runtime.Caller(1)
		log.Fatalf("[error] %s:%d %v", filename, line, err)
	}
	return
}

func dbInit() (err error) {
	dbFileName := "./data/go-sql-test.sqlite"
	log.Printf("dbFilename = %s\n", dbFileName)

	if _, err := os.Stat(dbFileName); err == nil {
		if err2 := os.Remove(dbFileName); err2 != nil {
			_, filename, line, _ := runtime.Caller(1)
			log.Fatalf("[error] %s:%d %v", filename, line, err)
		}
	}

	dsn := dbFileName
	dsn += "?cache=shared&_journal_mode=WAL"
	dsn += "&_synchronous=NORMAL" // OFF added for testing
	log.Printf("dsn = %s", dsn)

	opt.db, err = sql.Open("sqlite3", dsn)
	if err != nil {
		log.Println("Error:", err)
		return
	}
	if err := opt.db.Ping(); err != nil {
		_, filename, line, _ := runtime.Caller(1)
		log.Fatalf("[error] %s:%d %v", filename, line, err)
	}

	err = dbCreateSchema()
	if err != nil {
		_, filename, line, _ := runtime.Caller(1)
		log.Fatalf("[error] %s:%d %v", filename, line, err)
	}
	opt.db.Close()

	opt.db, err = sql.Open("sqlite3", dsn)
	if err != nil {
		_, filename, line, _ := runtime.Caller(1)
		log.Fatalf("[error] %s:%d %v", filename, line, err)
	}

	return
}

func genData() (fakeData []model.User, err error) {
	for i := 0; i < *opt.rowCount; i++ {
		a := structFakeData{}
		err := faker.FakeData(&a)
		if err != nil {
			_, filename, line, _ := runtime.Caller(1)
			log.Fatalf("[error] %s:%d %v", filename, line, err)
		}
		im := `@` + a.User
		s := model.User{
			User:      a.User,
			City:      &a.Address.City,
			Region:    &a.Address.State,
			Country:   &a.Country,
			AreaCode:  getPtrNullableStringFromInt(a.AreaCode),
			ZipCode:   getPtrNullableStringFromInt(a.ZipCode),
			YearBirth: &a.YearBirth,
			Im:        &im,
			Name:      &a.Name,
		}
		fakeData = append(fakeData, s)
	}
	return
}

func insertWithRawSQLUpsert(data []model.User) {
	log.Println("Executing insertWithRawSQLUpsert")

	var tx *sql.Tx
	if *opt.useTransaction {
		// Get a Tx for making transaction requests.
		var err error
		tx, err = opt.db.Begin()
		if err != nil {
			_, filename, line, _ := runtime.Caller(1)
			log.Fatalf("[error] %s:%d %v", filename, line, err)
		}
		// Defer a rollback in case anything fails.
		defer tx.Rollback()
	}

	upsertUser := models.StmtUpsertUser(opt.db)
	bar := progressbar.Default(int64(len(data)))
	for _, rec := range data {
		if *opt.useTransaction {
			_, err := tx.Stmt(upsertUser()).Exec(rec.User, rec.City, rec.Region, rec.Country, rec.AreaCode,
				rec.ZipCode, rec.YearBirth, rec.Im, rec.Name)
			if err != nil {
				_, filename, line, _ := runtime.Caller(1)
				log.Fatalf("[error] %s:%d %v", filename, line, err)
			}
		} else {
			_, err := upsertUser().Exec(rec.User, rec.City, rec.Region, rec.Country, rec.AreaCode,
				rec.ZipCode, rec.YearBirth, rec.Im, rec.Name)
			if err != nil {
				_, filename, line, _ := runtime.Caller(1)
				log.Fatalf("[error] %s:%d %v", filename, line, err)
			}
		}
		bar.Add(1)
	}
	bar.Finish()

	if *opt.useTransaction {
		log.Print("Commit Start")
		err := tx.Commit()
		if err != nil {
			_, filename, line, _ := runtime.Caller(1)
			log.Fatalf("[error] %s:%d %v", filename, line, err)
		}
		log.Print("Commit Finished")
	}
}

func updateWithRawSQLUpsert(data []model.User) {
	if *opt.updateCount == 0 {
		return
	}
	log.Println("Executing updateWithRawSQLUpsert")

	var tx *sql.Tx
	if *opt.useTransaction {
		// Get a Tx for making transaction requests.
		var err error
		tx, err = opt.db.Begin()
		if err != nil {
			_, filename, line, _ := runtime.Caller(1)
			log.Fatalf("[error] %s:%d %v", filename, line, err)
		}
		// Defer a rollback in case anything fails.
		defer tx.Rollback()
	}

	updateCount := 0
	upsertUser := models.StmtUpsertUser(opt.db)
	bar := progressbar.Default(int64(*opt.updateCount)) //(len(data)))
	for _, rec := range data {
		updateCount++
		if updateCount >= *opt.updateCount {
			break
		}
		*rec.YearBirth--
		if *opt.useTransaction {
			_, err := tx.Stmt(upsertUser()).Exec(rec.User, rec.City, rec.Region, rec.Country, rec.AreaCode,
				rec.ZipCode, rec.YearBirth, rec.Im, rec.Name)
			if err != nil {
				_, filename, line, _ := runtime.Caller(1)
				log.Fatalf("[error] %s:%d %v", filename, line, err)
			}
		} else {
			_, err := upsertUser().Exec(rec.User, rec.City, rec.Region, rec.Country, rec.AreaCode,
				rec.ZipCode, rec.YearBirth, rec.Im, rec.Name)
			if err != nil {
				_, filename, line, _ := runtime.Caller(1)
				log.Fatalf("[error] %s:%d %v", filename, line, err)
			}
		}
		bar.Add(1)
	}
	bar.Finish()

	if *opt.useTransaction {
		log.Print("Commit Start")
		err := tx.Commit()
		if err != nil {
			_, filename, line, _ := runtime.Caller(1)
			log.Fatalf("[error] %s:%d %v", filename, line, err)
		}
		log.Print("Commit Finished")
	}
}

func selectWithRawSQLUpsert(data []model.User) {
	log.Println("Executing selectWithRawSQLUpsert")

	var tx *sql.Tx
	if *opt.useTransaction {
		// Get a Tx for making transaction requests.
		var err error
		tx, err = opt.db.Begin()
		if err != nil {
			_, filename, line, _ := runtime.Caller(1)
			log.Fatalf("[error] %s:%d %v", filename, line, err)
		}
		// Defer a rollback in case anything fails.
		defer tx.Rollback()
	}

	selectUser := models.StmtSelectUser(opt.db)
	bar := progressbar.Default(int64(len(data)))
	for _, rec := range data {
		var row models.RawSqlUser
		if *opt.useTransaction {
			err := tx.Stmt(selectUser()).QueryRow(rec.User).Scan(&row.User, &row.City, &row.Region, &row.Country, &row.AreaCode,
				&row.ZipCode, &row.YearBirth, &row.Im, &row.Name, &row.CreatedTst, &row.ChangedTst)
			if err != nil {
				if err == sql.ErrNoRows {
					_, filename, line, _ := runtime.Caller(1)
					log.Printf("[warning] %s:%d %v in for user = %s", filename, line, err, rec.User)
				}
				_, filename, line, _ := runtime.Caller(1)
				log.Fatalf("[error] %s:%d %v", filename, line, err)
			}
		} else {
			err := selectUser().QueryRow(rec.User).Scan(&row.User, &row.City, &row.Region, &row.Country, &row.AreaCode,
				&row.ZipCode, &row.YearBirth, &row.Im, &row.Name, &row.CreatedTst, &row.ChangedTst)
			if err != nil {
				if err == sql.ErrNoRows {
					_, filename, line, _ := runtime.Caller(1)
					log.Printf("[warning] %s:%d %v in for user = %s", filename, line, err, rec.User)
				}
				_, filename, line, _ := runtime.Caller(1)
				log.Fatalf("[error] %s:%d %v", filename, line, err)
			}
		}
		bar.Add(1)
	}
	bar.Finish()

	if *opt.useTransaction {
		log.Print("Commit Start")
		err := tx.Commit()
		if err != nil {
			_, filename, line, _ := runtime.Caller(1)
			log.Fatalf("[error] %s:%d %v", filename, line, err)
		}
		log.Print("Commit Finished")
	}
}

func insertWithJet(data []model.User) {
	log.Println("Executing insertWithJet")

	var tx *sql.Tx
	if *opt.useTransaction {
		// Get a Tx for making transaction requests.
		var err error
		tx, err = opt.db.Begin()
		if err != nil {
			_, filename, line, _ := runtime.Caller(1)
			log.Fatalf("[error] %s:%d %v", filename, line, err)
		}
		// Defer a rollback in case anything fails.
		defer tx.Rollback()
	}

	bar := progressbar.Default(int64(len(data)))
	for _, rec := range data {

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
						OR(User.Name.IS_DISTINCT_FROM(User.EXCLUDED.Name)),
				),
			)

		// sql_debug := stmtInserUser.DebugSql()
		// fmt.Println(sql_debug)
		if *opt.useTransaction {
			_, err := stmtInserUser.Exec(tx)
			if err != nil {
				_, filename, line, _ := runtime.Caller(1)
				log.Fatalf("[error] %s:%d %v", filename, line, err)
			}
		} else {
			_, err := stmtInserUser.Exec(opt.db)
			if err != nil {
				_, filename, line, _ := runtime.Caller(1)
				log.Fatalf("[error] %s:%d %v", filename, line, err)
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

func updateWithJet(data []model.User) {
	if *opt.updateCount == 0 {
		return
	} 
	log.Println("Executing updateWithJet")

	var tx *sql.Tx
	if *opt.useTransaction {
		// Get a Tx for making transaction requests.
		var err error
		tx, err = opt.db.Begin()
		if err != nil {
			_, filename, line, _ := runtime.Caller(1)
			log.Fatalf("[error] %s:%d %v", filename, line, err)
		}
		// Defer a rollback in case anything fails.
		defer tx.Rollback()
	}

	updateCount := 0
	bar := progressbar.Default(int64(*opt.updateCount)) //(len(data))) 
	for _, rec := range data { 
		updateCount++
		if updateCount >= *opt.updateCount {
			break
		}
		*rec.YearBirth-- 

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
						OR(User.Name.IS_DISTINCT_FROM(User.EXCLUDED.Name)),
				),
			)

		// sql_debug := stmtInserUser.DebugSql()
		// fmt.Println(sql_debug)
		if *opt.useTransaction {
			_, err := stmtInserUser.Exec(tx)
			if err != nil {
				_, filename, line, _ := runtime.Caller(1)
				log.Fatalf("[error] %s:%d %v", filename, line, err)
			}
		} else {
			_, err := stmtInserUser.Exec(opt.db)
			if err != nil {
				_, filename, line, _ := runtime.Caller(1)
				log.Fatalf("[error] %s:%d %v", filename, line, err)
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

func selectWithJet(data []model.User) {
	log.Println("Executing selectWithJet")

	var tx *sql.Tx
	if *opt.useTransaction {
		// Get a Tx for making transaction requests.
		var err error
		tx, err = opt.db.Begin()
		if err != nil {
			_, filename, line, _ := runtime.Caller(1)
			log.Fatalf("[error] %s:%d %v", filename, line, err)
		}
		// Defer a rollback in case anything fails.
		defer tx.Rollback()
	}

	bar := progressbar.Default(int64(len(data)))
	for _, rec := range data {

		columnList := ColumnList{
			User.User, User.City, User.Region, User.Country, User.AreaCode, User.ZipCode,
			User.YearBirth, User.Im, User.Name, User.CreatedTst, User.ChangedTst,
		}

		stmtInserUser := User.SELECT(columnList).
			FROM(User).
			WHERE(User.User.EQ(String(rec.User)))

		// sql_debug := stmtInserUser.DebugSql()
		// fmt.Println(sql_debug)
		if *opt.useTransaction {
			_, err := stmtInserUser.Exec(tx)
			if err != nil {
				_, filename, line, _ := runtime.Caller(1)
				log.Fatalf("[error] %s:%d %v", filename, line, err)
			}
		} else {
			_, err := stmtInserUser.Exec(opt.db)
			if err != nil {
				_, filename, line, _ := runtime.Caller(1)
				log.Fatalf("[error] %s:%d %v", filename, line, err)
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

func main() {
	log.Println("Execution Starting")

	cpuprofile := flag.String("cpuprofile", "", "write cpu profile to file")
	opt.flipYearBirth = flag.Bool("flipYearBirth", false, "Flip YearBirth generation for Upsert Testing")
	opt.rowCount = flag.Int("rowCount", 10000, "Number of rows to use in test")
	opt.updateCount = flag.Int("updateCount", 1000, "Maximum number of updates to perform")
	opt.useBoth = flag.Bool("useBoth", false, "Run both RawSql and Jet")
	opt.useJet = flag.Bool("useJet", false, "Run using Jet module")
	opt.useRawSQL = flag.Bool("useRawSQL", false, "Run using RawSQL module")
	opt.useTransaction = flag.Bool("useTransaction", false, "Wrap work in transaction")

	flag.Parse()

	if *opt.useBoth {
		*opt.useRawSQL = true
		*opt.useJet = true
	} else if *opt.useJet {
		*opt.useRawSQL = false
	} else {
		*opt.useRawSQL = true
	}

	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			_, filename, line, _ := runtime.Caller(1)
			log.Fatalf("[error] %s:%d %v", filename, line, err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	err := dbInit()
	if err != nil {
		_, filename, line, _ := runtime.Caller(1)
		log.Fatalf("[error] %s:%d %v", filename, line, err)
	}
	defer opt.db.Close()

	err = dbCleanUp()
	if err != nil {
		_, filename, line, _ := runtime.Caller(1)
		log.Fatalf("[error] %s:%d %v", filename, line, err)
	}

	data, err := genData()
	if err != nil {
		_, filename, line, _ := runtime.Caller(1)
		log.Fatalf("[error] %s:%d %v", filename, line, err)
	}

	log.Println("Sort data Starting")
	sort.Slice(data, func(i, j int) bool {
		return data[i].User < data[j].User
	})
	log.Println("Sort data Ended")

	if *opt.useRawSQL {
		insertWithRawSQLUpsert(data)
		updateWithRawSQLUpsert(data)
		selectWithRawSQLUpsert(data)
	}

	if *opt.useJet {
		if *opt.useBoth {
			err = dbCleanUp()
			if err != nil {
				_, filename, line, _ := runtime.Caller(1)
				log.Fatalf("[error] %s:%d %v", filename, line, err)
			}
			log.Print("Reset database for Jet")
		}
		insertWithJet(data)
		updateWithJet(data)
		selectWithJet(data)
	}

	log.Println("Exection Completed")
}
