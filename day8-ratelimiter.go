
package main

import (

	"fmt"
	"net/http"
  "time"
  "sync"

)
 // userID -> request times
var requests=make(map[string][]time.Time)
var mu sync.Mutex

func handler(w http.ResponseWriter, r *http.Request) {

  userID:= r.URL.Query().Get("user")

  
  now := time.Now()
  cutoff := now.Add(-time.Minute)

   mu.Lock()
  defer mu.Unlock()
  var valid[]time.Time
  for _,t := range  requests[userID]{
    if t.After(cutoff){
      valid = append(valid,t)
    }
  }
   if len(valid) <100{
    fmt.Fprintln(w,"allow")
    requests[userID] =append(valid,now)
   } else{
    fmt.Fprintln(w,"reject")
   }
}

func main(){
 http.HandleFunc("/", handler)

	http.ListenAndServe(":8080", nil)

}