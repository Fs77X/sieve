package main
import (
	"fmt"
	_ "github.com/lib/pq"
	"database/sql"
	"math/rand"
	"time"
	"strconv"
)
// Currently hosting db server locally, db credentials below
const (
	DB_USER     = "sieve"
	DB_PASSWORD = ""
	DB_NAME     = "sieve"
)


type mData struct {
	TTL       int64 `json:"TTL"`
	Purpose   string `json:"purpose"`
	Origin    string `json:"origin"`
	Objection string `json:"objection"`
	Sharing   string `json:"sharing"`
}

var counter int = 10000
var ENFORCEMENT_ACTION string = "allow"

func generateMData() mData {
	var newMData mData
	newMData.TTL = time.Now().Unix() + int64(rand.Intn(97000) + 3000)
	newMData.Purpose = "purpose" + strconv.Itoa(rand.Intn(99) + 1)
	newMData.Origin = "src" + strconv.Itoa(rand.Intn(99) + 1)
	newMData.Objection = "obj" + strconv.Itoa(rand.Intn(99) + 1)
	newMData.Sharing = "shr" + strconv.Itoa(rand.Intn(99) + 1)
	return newMData
}

func insertPolicy(listOfIds []string, db *sql.DB) {
	for _, id := range listOfIds{
		gpdrMeta := generateMData()
		querier := rand.Intn(39) + 1
		inserted_at := time.Now()
		_, err := db.Exec(`INSERT INTO user_policy(policy_id, id, querier,
			purpose, ttl, origin, objection, sharing,
			enforcement_action, inserted_at)
			VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10);`, counter, id, querier, gpdrMeta.Purpose, gpdrMeta.TTL, gpdrMeta.Origin, gpdrMeta.Objection, gpdrMeta.Sharing, ENFORCEMENT_ACTION, inserted_at)
		checkErr(err)
		counter = counter + 1
	}
}

func yoinkUUID(db *sql.DB) []string{
	rows, err := db.Query("SELECT distinct policy_id from user_policy_object_condition;")
	checkErr(err)
	var res []string
	for rows.Next() {
		var policy_id string
		err1 := rows.Scan(&policy_id)
		checkErr(err1)
		res = append(res, policy_id)
	}
	return res
}

func delOldmData(db *sql.DB) {
	_, err := db.Query("DELETE FROM user_policy;")
	checkErr(err)
}

// TODO: SELECT distinct policy_id from user_policy_object_condition
// Generate random metadata for each policy_id
// INSERT into user_policy() VALUES()
func main(){
	db := setupDB()
	delOldmData(db)
	listOfIds := yoinkUUID(db)
	insertPolicy(listOfIds, db)
	fmt.Println("DONE :)")

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