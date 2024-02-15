package models

import (
	"database/sql"
	"log"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

type RawSqlUser struct {
	User       string 
	City       *string
	Region     *string
	Country    *string
	AreaCode   *string
	ZipCode    *string
	YearBirth  *int32
	Im         *string
	Name       *string
	CreatedTst *time.Time
	ChangedTst *time.Time
}

func StmtUpsertUser(db *sql.DB) func() *sql.Stmt {
	const sqlUpsertUser string = `
		INSERT INTO user (
			user
			, city
			, region
			, country
			, area_code
			, zip_code
			, year_birth
			, im
			, name
		)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT (user)	
		DO UPDATE 
		      SET city        = excluded.city
			    , region      = excluded.region
			    , country     = excluded.country
			    , area_code   = excluded.area_code
			    , zip_code    = excluded.zip_code
			    , year_birth  = excluded.year_birth
			    , im          = excluded.im
			    , name        = excluded.name
		    WHERE city       IS NOT excluded.city
		       OR region      IS NOT excluded.region
		       OR country     IS NOT excluded.country
		       OR area_code   IS NOT excluded.area_code
		       OR zip_code    IS NOT excluded.zip_code
		       OR year_birth  IS NOT excluded.year_birth
		       OR im          IS NOT excluded.im
		       OR name        IS NOT excluded.name
	;`

	stmt, err := db.Prepare(sqlUpsertUser)
	if err != nil {
		log.Fatal(err)
	}

	return func() *sql.Stmt {
		return stmt
	}
}

func StmtSelectUser(db *sql.DB) func() *sql.Stmt {
	const sqlSelectUser string = `
		SELECT user
			 , city 
			 , region 
			 , country 
			 , area_code 
			 , zip_code 
			 , year_birth 
			 , im 
			 , name
			 , created_tst
			 , changed_tst
		  FROM user
		 WHERE "user" = ?
		;`

	stmt, err := db.Prepare(sqlSelectUser)
	if err != nil {
		log.Fatal(err)
	}

	return func() *sql.Stmt {
		return stmt
	}
}
