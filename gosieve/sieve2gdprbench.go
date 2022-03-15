// get device id/entry
// translate into one entire row
// insert into new table
package main

import (
	"database/sql"
	"fmt"
	// "math/rand"
	// "strconv"
	// "time"

	_ "github.com/lib/pq"
	// uuid "github.com/satori/go.uuid"
)

type dataPair struct {
	mallData md
	metaData mData
}

type md struct {
	Id            string `json:"id"`
	Shop_Name     string `json:"shop_name"`
	Obs_Date      string `json:"obs_date"`
	Obs_Time      string `json:"obs_time"`
	User_Interest string `json:"user_interest"`
	Device_Id     int    `json:"device_id"`
}
// `json:""`
type mData struct {
	Policy_Id string `json:"policy_id"`
	Id		  string `json:"id"`	
	Querier   string `json:"querier"`
	Purpose   string `json:"purpose"`
	TTL       int64  `json:"TTL"`
	Origin    string `json:"origin"`
	Objection string `json:"objection"`
	Sharing   string `json:"sharing"`
	Enforcement_Action string `json:"enforcement_action"`
	Inserted_At string `json:"inserted_at"`
	Device_Id int `json:"device_id"`
	Key       string `json:"key"`
}

const (
	SIEVE_USER     = "sieve"
	SIEVE_PASSWORD = ""
	SIEVE_NAME     = "sieve"
)

const (
	DB_USER = "postgres"
	DB_PASSWORD = "admin"
	DB_NAME = "the_db"
)

func MDColMallData(colname string, newMD *md) interface{} {
	switch colname {
	case "id":
		return &newMD.Id
	case "shop_name":
		return &newMD.Shop_Name
	case "obs_date":
		return &newMD.Obs_Date
	case "obs_time":
		return &newMD.Obs_Time
	case "user_interest":
		return &newMD.User_Interest
	case "device_id":
		return &newMD.Device_Id
	default:
		panic("unknown column " + colname)
	}
}

func MDColMeta(colname string, newMD *mData) interface{} {
	switch colname {
	case "policy_id":
		return &newMD.Policy_Id
	case "id":
		return &newMD.Id
	case "querier":
		return &newMD.Querier
	case "purpose":
		return &newMD.Purpose
	case "ttl":
		return &newMD.TTL
	case "origin":
		return &newMD.Origin
	case "objection":
		return &newMD.Objection
	case "sharing":
		return &newMD.Sharing
	case "enforcement_action":
		return &newMD.Enforcement_Action
	case "inserted_at":
		return &newMD.Inserted_At
	case "device_id":
		return &newMD.Device_Id
	case "key":
		return &newMD.Key
	default:
		panic("unknown column " + colname)
	}
}


func yoinkMD(db *sql.DB, entry string) mData {
	rows, err := db.Query("SELECT * FROM user_policy WHERE key = $1;", entry)
	checkErr(err)
	var columns []string
	columns, err1 := rows.Columns()
	checkErr(err1)
	var res mData
	colNum := len(columns)
	for rows.Next() {
		var newMetaData mData
		cols := make([]interface{}, colNum)
		for i := 0; i < colNum; i++ {
			cols[i] = MDColMeta(columns[i], &newMetaData)
		}

		err2 := rows.Scan(cols...)
		checkErr(err2)
		res = newMetaData
	}
	return res

}

func generatePairs(db *sql.DB, mallData []md, baseDB *sql.DB, limit int) {
	for i, data := range mallData {
		if(i == limit) {
			break
		}
		fmt.Println("On iteration: ", i)
		var dp dataPair
		dp.mallData = data
		metaData := yoinkMD(db, data.Id)
		dp.metaData = metaData
		generateBase(baseDB, dp)
	}
}

func yoinkData(db *sql.DB) []md {
	rows, err := db.Query("SELECT * FROM mall_observation;")
	checkErr(err)
	var columns []string
	columns, err1 := rows.Columns()
	checkErr(err1)
	var res []md
	colNum := len(columns)
	for rows.Next() {
		var newMd md
		cols := make([]interface{}, colNum)
		for i := 0; i < colNum; i++ {
			cols[i] = MDColMallData(columns[i], &newMd)
		}

		err2 := rows.Scan(cols...)
		checkErr(err2)
		res = append(res, newMd)
	}
	return res
}

func generateBase(db *sql.DB, pair dataPair) {
	_, err := db.Exec(`INSERT INTO usertable(id, shop_name, obs_date,
		obs_time, user_interest, device_id, querier, purpose,
		ttl, origin, objection, sharing, enforcement_action, inserted_at)
		VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14);`, pair.mallData.Id,
	pair.mallData.Shop_Name, pair.mallData.Obs_Date, pair.mallData.Obs_Time, pair.mallData.User_Interest, pair.mallData.Device_Id,
	pair.metaData.Querier, pair.metaData.Purpose, pair.metaData.TTL, pair.metaData.Origin, pair.metaData.Objection, pair.metaData.Sharing,
	pair.metaData.Enforcement_Action, pair.metaData.Inserted_At)
	checkErr(err)
	
}

func main() {
	sieveDB := setupSieveDB()
	baseDB := setupDB()
	mallData := yoinkData(sieveDB)
	fmt.Println("yoinked md")
	generatePairs(sieveDB, mallData, baseDB, 50000)
	// fmt.Println("yoinked meta")
	// // generateBase(baseDB, dp)
	fmt.Println("Finished pushing data")

}

func setupSieveDB() *sql.DB {
	dbinfo := fmt.Sprintf("user=%s password=%s dbname=%s sslmode=disable", SIEVE_USER, SIEVE_PASSWORD, SIEVE_NAME)
	db, err := sql.Open("postgres", dbinfo)

	checkErr(err)

	return db
}

func setupDB() *sql.DB {
	// fmt.Println("user=%s password=%s dbname=%s sslmode=disable", DB_USER, DB_PASSWORD, DB_NAME)
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

