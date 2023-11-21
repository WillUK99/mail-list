package db

import (
	"database/sql"
	"log"
	"time"

	"github.com/mattn/go-sqlite3"
)

type EmailEntry struct {
	Id          int64
	Email       string
	ConfirmedAt *time.Time
	OptOut      bool
}

func Create(db *sql.DB) {
	_, err := db.Exec(`
		CREATE TABLE emails (
			id 						INTEGER PRIMARY KEY,
			email 				TEXT UNIQUE,
			confirmed_at	INTEGER,
			opt_out 			BOOLEAN NOT NULL DEFAULT FALSE
		);
	`)
	if err != nil {
		if sqlError, ok := err.(sqlite3.Error); ok {
			if sqlError.Code != 1 { // 1 = SQLITE_ERROR = table already exists
				log.Fatal(sqlError)
			}
		} else {
			log.Fatal(err)
		}
	}
}

func emailEntry(row *sql.Rows) (*EmailEntry, error) {
	var id int64
	var email string
	var confirmedAt int64
	var optOut bool

	err := row.Scan(&id, &email, &confirmedAt, &optOut)

	if err != nil {
		log.Println(err)
		return nil, err
	}

	t := time.Unix(confirmedAt, 0)

	return &EmailEntry{
		Id:          id,
		Email:       email,
		ConfirmedAt: &t,
		OptOut:      optOut,
	}, nil
}

func CreateEmail(db *sql.DB, email string) error {
	_, err := db.Exec(`
		INSERT INTO emails (email, confirmed_at, opt_out) 
		VALUES (?, 0, false`, email)

	if err != nil {
		log.Println(err)
		return err
	}

	return nil
}

func GetEmail(db *sql.DB, email string) (*EmailEntry, error) {
	rows, err := db.Query(`
		SELECT id, email, confirmed_at, opt_out 
		FROM emails 
		WHERE email = ?`, email)

	if err != nil {
		log.Println(err)
		return nil, err
	}

	defer rows.Close() // need to close the database connection when the function returns :)

	if rows.Next() {
		return emailEntry(rows)
	}

	return nil, nil
}

func UpdateEmail(db *sql.DB, entry EmailEntry) error {
	t := time.Now().Unix()

	_, err := db.Exec(`
		INSERT INTO emails (email, confirmed_at, opt_out)
		VALUES (?, ?, false)
		ON CONFLICT(email) DO UPDATE SET confirmed_at = ? opt_out = ? `, entry.Email, t, entry.OptOut, t, entry.OptOut)

	if err != nil {
		log.Println(err)
		return err
	}

	return nil
}

// This app is a mailing list so doesn't actually delete emails, it just sets the opt_out flag to true
func DeleteEmail(db *sql.DB, email string) error {
	_, err := db.Exec(`
		UPDATE emails
		SET opt_out = true
		WHERE email = ?`, email)

	if err != nil {
		log.Println(err)
		return err
	}

	return nil
}

type GetEmailBatchQueryParams struct {
	Offset int
	Limit  int
}

func GetEmailBatch(db *sql.DB, params GetEmailBatchQueryParams) ([]EmailEntry, error) {
	rows, err := db.Query(`
		SELECT id, email, confirmed_at, opt_out
		FROM emails
		ORDER BY id ASC
		LIMIT ? OFFSET ?`, params.Limit, (params.Offset-1)*params.Limit)

	if err != nil {
		log.Println(err)
		return nil, err
	}

	defer rows.Close()

	emails := make([]EmailEntry, 0, params.Limit)

	for rows.Next() {
		entry, err := emailEntry(rows)

		if err != nil {
			log.Println(err)
			return nil, err
		}

		emails = append(emails, *entry)
	}

	return emails, nil
}
