package main

import (
	"fmt"
	"encoding/json"
	"net/http"
	"log"
	"github.com/gorilla/mux"
	_ "github.com/lib/pq"
	"database/sql"
	"github.com/google/uuid"
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

type JsonResponse struct {
	Type    string           `json:"type"`
	Message string           `json:"message"`
}

type mData struct {
	TTL       string `json:"TTL"`
	Purpose   string `json:"purpose"`
	Adm       string `jsong:"adm"`
	Origin    string `json:"origin"`
	Objection string `json:"objection"`
	Sharing   string `json:"sharing"`
	ACL       string `json:"acl"`
	CAT       string `json:"cat"`
}

var counter int = 1000
var counter1 int = 10000
func main(){
	router := mux.NewRouter()
	
	// Add data
	router.HandleFunc("/madd_obj/", createEntry).Methods("POST")

	// Add metadata
	router.HandleFunc("/madd_metaobj/{queryid}", addMetaData).Methods("POST")
	log.Fatal(http.ListenAndServe(":8000", router))
}

func add_policy(data mData, db *sql.DB) (string, int) {
	// TODO: add uuid gen, getting data in
	uniqueid := uuid.New().String()
	q := strconv.Itoa(rand.Intn(99) + 1)
	enf_action := "allow"
	currTime := time.Now() //.String()
	_, err := db.Exec(`INSERT INTO user_policy(policy_id, id, querier,
		 purpose, ttl, adm, origin, objection, sharing,
		 enforcement_action, inserted_at)
		 VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11);`, counter, uniqueid, q, data.Purpose, data.TTL, data.Adm, data.Origin, data.Objection, data.Sharing, enf_action, currTime)
	checkErr(err)
	counter = counter + 1
	return uniqueid, 0

}

func getuIdData(mID string,db *sql.DB) (string, string) {
	rows, err := db.Query("SELECT userid, dataVal FROM gdpr_data WHERE id = $1;", mID)
	checkErr(err)
	var resUserId string
	var resData string
	for rows.Next() {
		var id string
		var dataVal string
		err1 := rows.Scan(&id, &dataVal)
		checkErr(err1)
		resUserId = id
		resData = dataVal
	}
	return resUserId, resData
}

//use same uuid from add_policy
func add_objcond(mID string, uID string, db *sql.DB) int {
	// TODO: getting data in with corresponding uuidgen
	attrib0 := "userid"
	attrib1 := "dataVal"
	attribute_type := "STRING"
	operator := "="
	compVal0, compVal1 := getuIdData(mID, db)
	_, err := db.Exec(`INSERT INTO user_policy_object_condition(id, policy_id, attribute, attribute_type, operator, comp_value) VALUES(
		$1, $2, $3, $4, $5, $6);`, counter1, uID, attrib0, attribute_type, operator, compVal0)
	checkErr(err)
	counter1 = counter1 + 1
	_, err1 := db.Exec(`INSERT INTO user_policy_object_condition(id, policy_id, attribute, attribute_type, operator, comp_value) VALUES(
		$1, $2, $3, $4, $5, $6);`, counter1, uID, attrib1, attribute_type, operator, compVal1)
	counter1 = counter1 + 1
	checkErr(err1)
	return 0
}

func addMetaData(w http.ResponseWriter, r *http.Request){
	params := mux.Vars(r)
	mID := params["queryid"]
	var data mData
	if err := json.NewDecoder(r.Body).Decode(&data); err != nil {
		fmt.Println(err)
	}
	var response = JsonResponse{}
	// response = JsonResponse{Type: "success", Message: "Metadata item has been added successfully!"}
	// json.NewEncoder(w).Encode(response)

	if mID == "" {
		status := 400
		w.WriteHeader(status)
		response = JsonResponse{Type: "error", Message: "You are missing id parameter."}
		json.NewEncoder(w).Encode(response)
		return
	}
	db := setupDB()
	//check if user doesn't exist
	if !userExist(mID, db) {
		status := 403
		w.WriteHeader(status)
		response = JsonResponse{Type: "error", Message: "Create user first!"}
		json.NewEncoder(w).Encode(response)
		db.Close()
		return
	}
	uID, res := add_policy(data, db)
	if res != 0 {
		status := 500
		w.WriteHeader(status)
		response = JsonResponse{Type: "error", Message: "Something wrong!"}
		json.NewEncoder(w).Encode(response)
		db.Close()
		return
	}
	res = add_objcond(mID, uID, db)
	if res != 0 {
		status := 500
		w.WriteHeader(status)
		response = JsonResponse{Type: "error", Message: "Something wrong!"}
		json.NewEncoder(w).Encode(response)
		db.Close()
		return
	}
	fmt.Println("gucci")
	status := 201
	w.WriteHeader(status)
	response = JsonResponse{Type: "Success", Message: "Metadata entered"}
	json.NewEncoder(w).Encode(response)
	db.Close()
	return

}

func userExist(entryID string, db *sql.DB) bool {
	rows, err := db.Query("SELECT id FROM gdpr_data WHERE id = $1;", entryID)
	checkErr(err)
	var res []string
	for rows.Next() {
		var id string
		var err1 = rows.Scan(&id)
		checkErr(err1)
		res = append(res, id)
	}
	if len(res) == 1 {
		return true
	}
	return false

}

func createEntry(w http.ResponseWriter, r *http.Request) {
	entryID := r.FormValue("id")
	entryName := r.FormValue("name")
	entryGpa := r.FormValue("gpa")
	fmt.Println(entryID, entryName, entryGpa)
	//bad request
	if entryID == "" || entryName == "" || entryGpa == "" {
		status := 400
		response := JsonResponse{Type: "Failure", Message: "Entries are missing!"}
		w.WriteHeader(status)
		json.NewEncoder(w).Encode(response)
		return
	}
	db := setupDB()
	//check if exist
	if userExist(entryID, db) {
		status := 403
		response := JsonResponse{Type: "Failure", Message: "Entries already exists!"}
		w.WriteHeader(status)
		json.NewEncoder(w).Encode(response)
		db.Close()
		return
	}
	//attempt to insert
	_, err := db.Exec("INSERT INTO gdpr_data(id, dataVal, userid) VALUES($1, $2, $3);", entryID, entryGpa, entryName)
	db.Close()
	checkErr(err)
	status := 201
	fmt.Println("gucci")
	response := JsonResponse{Type: "success", Message: "The entry has been inserted successfully!"}
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(response)

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


