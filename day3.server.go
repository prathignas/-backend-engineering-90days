package main

import (
	"fmt"
	"net/http"
	"encoding/json"
)
var count =0

func hello(w http.ResponseWriter , req *http.Request){
	fmt.Fprintf(w,"hello")
}


func Count(w http.ResponseWriter , req *http.Request){
	w.Header().Set("Content-Type", "application/json")
	result := map[string]int {"count":count}
	json.NewEncoder(w).Encode(result)
}

func increament(w http.ResponseWriter , req *http.Request){
	
	if req.Method != "POST"{
		w.WriteHeader(405)
		return 
	}
	var body IncrementRequest
    json.NewDecoder(req.Body).Decode(&body)
    count += body.Amount

	w.Header().Set("Content-Type", "application/json")
	result := map[string]int {"count":count}
	json.NewEncoder(w).Encode(result)

}
 type IncrementRequest struct{
	Amount int `json:"amount"`
 }

func main(){
  http.HandleFunc("/hello",hello)
  http.HandleFunc("/count",Count)
  http.HandleFunc("/increment",increament)
  http.ListenAndServe(":8090",nil)
}