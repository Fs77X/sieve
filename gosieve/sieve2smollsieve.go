// get data full of ids on mall_observation
// split keys to k and use latter part as deletion list
// get policy id of each key from mal_o and then delete from user_policy
// perform deletion on list of policy id in object conditions
package main

import (
	"database/sql"
	"fmt"
	_ "github.com/lib/pq"
	"strconv"
)

const (
	DB_USER     = "sieve"
	DB_PASSWORD = ""
	DB_NAME     = "sieve"
)

func getOCID(db *sql.DB, key string) string {
	rows, err := db.Query("SELECT id FROM user_policy WHERE key = $1;", key)
	checkErr(err)
	var ocid string
	for rows.Next() {
		var id string
		err1 := rows.Scan(&id)
		checkErr(err1)
		ocid = id
	}
	return ocid
}

func deleteOC(db *sql.DB, policy_id string) {
	_, err := db.Exec("DELETE FROM user_policy_object_condition WHERE policy_id = $1;", policy_id)
	checkErr(err)
}

func deleteUserPolicy(db *sql.DB, key string) {
	_, err := db.Exec("DELETE FROM user_policy WHERE key = $1;", key)
	checkErr(err)

}

func deletePolicy(db *sql.DB, terminateIds []string) {
	for i, id := range terminateIds {
		ocid := getOCID(db, id)
		deleteData(db, id)
		deleteUserPolicy(db, id)
		deleteOC(db, ocid)
		fmt.Println(strconv.Itoa(i) + " of " + strconv.Itoa(len(terminateIds)))
	}

}

func deleteData(db *sql.DB, key string) {
	_, err := db.Exec("DELETE FROM mall_observation WHERE id = $1;", key)
	checkErr(err)
}

func yoinkMallDBIDS(db *sql.DB, limit int)[] string {
	var res []string
	rows, err := db.Query("SELECT id FROM mall_observation;")
	checkErr(err)
	for rows.Next() {
		var id string
		err1 := rows.Scan(&id)
		checkErr(err1)
		res = append(res, id)
	}
	fmt.Println(len(res)-limit)
	return res[limit:]

}
func main() {
	db := setupDB()
	listofIds := yoinkMallDBIDS(db, 10000)
	fmt.Println(len(listofIds))
	deletePolicy(db, listofIds)
}

// DB set up
func setupDB() *sql.DB {
	dbinfo := fmt.Sprintf("user=%s password=%s dbname=%s sslmode=disable", DB_USER, DB_PASSWORD, DB_NAME)
	db, err := sql.Open("postgres", dbinfo)

	checkErr(err)

	return db
}

// Function for handling errors
func checkErr(err error) {
	if err != nil {
		panic(err)
	}
}
