package main

import (
	"database/sql"
	"fmt"
	"math/rand"
	"strconv"
	"time"

	_ "github.com/lib/pq"
	uuid "github.com/satori/go.uuid"
)

// Currently hosting db server locally, db credentials below
const (
	DB_USER     = "sieve"
	DB_PASSWORD = ""
	DB_NAME     = "sieve"
)

type mData struct {
	TTL       int64  `json:"TTL"`
	Purpose   string `json:"purpose"`
	Origin    string `json:"origin"`
	Objection string `json:"objection"`
	Sharing   string `json:"sharing"`
}

type OC struct {
	Id             int    `json:"id"`
	UUID           string `json:"uuid"`
	Attribute      string `json:"attribute"`
	Attribute_Type string `json:"attribute_type"`
	Operator       string `json:"operator"`
	Comp_Value     string `json:"comp_value"`
}

type md struct {
	Id            string `json:"id"`
	Shop_Name     string `json:"shop_name"`
	Obs_Date      string `json:"obs_date"`
	Obs_Time      string `json:"obs_time"`
	User_Interest string `json:"user_interest"`
	Device_Id     int    `json:"device_id"`
}

var counter int = 10000
var counterOC int = 594027
var ENFORCEMENT_ACTION string = "allow"
var attributes = [5]string{"device_id", "shop_name", "obs_date", "obs_time", "user_interest"}

func generateMData() mData {
	var newMData mData
	newMData.TTL = time.Now().Unix() + int64(rand.Intn(97000)+3000)
	newMData.Purpose = "purpose" + strconv.Itoa(rand.Intn(99)+1)
	newMData.Origin = "src" + strconv.Itoa(rand.Intn(99)+1)
	newMData.Objection = "obj" + strconv.Itoa(rand.Intn(99)+1)
	newMData.Sharing = "shr" + strconv.Itoa(rand.Intn(99)+1)
	return newMData
}

func insertOC(newOC OC, db *sql.DB) {
	_, err := db.Exec("INSERT INTO user_policy_object_condition(id, policy_id, attribute, attribute_type, operator, comp_value) VALUES($1, $2, $3, $4, $5, $6)", newOC.Id, newOC.UUID, newOC.Attribute, newOC.Attribute_Type, newOC.Operator, newOC.Comp_Value)
	checkErr(err)
}

func generateOC(id int, uuid string, attribute string, operator string, data md, db *sql.DB) {
	var newOC OC
	newOC.Id = id
	newOC.UUID = uuid
	newOC.Attribute = attribute
	switch attribute {
	case "obs_date":
		newOC.Attribute_Type = "DATE"
		layout := "2006-01-02T15:04:05Z"
		t, err2 := time.Parse(layout, data.Obs_Date)
		checkErr(err2)
		newOC.Comp_Value = t.Format("2006-01-02")
	case "obs_time":
		newOC.Attribute_Type = "TIME"
		layout := "2006-01-02T15:04:05Z"
		t, err2 := time.Parse(layout, data.Obs_Time)
		checkErr(err2)
		// date = t.Format("2006-01-02")
		// date = t.Format("15:04:05")
		newOC.Comp_Value = t.Format("15:04:05")
	case "shop_name":
		newOC.Attribute_Type = "STRING"
		newOC.Comp_Value = data.Shop_Name
	case "device_id":
		newOC.Attribute_Type = "STRING"
		newOC.Comp_Value = strconv.Itoa(data.Device_Id)
	case "user_interest":
		newOC.Attribute_Type = "STRING"
		newOC.Comp_Value = data.User_Interest
	}
	newOC.Operator = operator
	insertOC(newOC, db)
}

func insertPolicy(listOfIds []string, db *sql.DB) {
	for _, id := range listOfIds {
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

func MDCol(colname string, newMD *md) interface{} {
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
func getDate(db *sql.DB) {
	rows, err := db.Query("SELECT obs_time FROM mall_observation;")
	checkErr(err)
	for rows.Next() {
		var date string
		rows.Scan(&date)
		layout := "2006-01-02T15:04:05Z"
		t, err2 := time.Parse(layout, date)
		checkErr(err2)
		date = t.Format("2006-01-02")
		date = t.Format("15:04:05")
		fmt.Println(date)
	}
}

// https://stackoverflow.com/questions/21821550/sql-scan-rows-with-unknown-number-of-columns-select-from
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
			cols[i] = MDCol(columns[i], &newMd)
		}

		err2 := rows.Scan(cols...)
		checkErr(err2)
		res = append(res, newMd)
	}
	return res
}

func delOldmData(db *sql.DB) {
	_, err := db.Query("DELETE FROM user_policy;")
	_, err2 := db.Query("DELETE FROM user_policy_object_condition;")
	checkErr(err)
	checkErr(err2)
}

func generateAllAtributes(data []md, db *sql.DB) []string {
	var policy_id []string
	for _, mallData := range data {
		u1 := uuid.NewV4().String()
		policy_id = append(policy_id, u1)
		id := counterOC
		for _, attribute := range attributes {
			if attribute == "obs_date" || attribute == "obs_time" {
				generateOC(id, u1, attribute, ">=", mallData, db)
				counterOC = counterOC + 1
				id = counterOC
				generateOC(id, u1, attribute, "<=", mallData, db)
				counterOC = counterOC + 1
				id = counterOC
			} else {
				if (attribute == "user_interest" && mallData.User_Interest != "") || attribute != "user_interest" {
					generateOC(id, u1, attribute, "=", mallData, db)
					counterOC = counterOC + 1
					id = counterOC
					generateOC(id, u1, attribute, "=", mallData, db)
					counterOC = counterOC + 1
					id = counterOC
				}
			}
		}
	}
	return policy_id
}

// TODO: SELECT distinct policy_id from user_policy_object_condition
// Generate random metadata for each policy_id
// INSERT into user_policy() VALUES()
func main() {
	// generateOC()
	db := setupDB()
	// getDate(db)
	delOldmData(db)
	mallData := yoinkData(db)
	listOfIds := generateAllAtributes(mallData, db)
	// listOfIds := yoinkUUID(db)
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
