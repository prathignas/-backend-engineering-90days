package main

import(
    "encoding/json"
    "net/http"
	"strconv"
)

type User struct {
    ID   int    `json:"id"`
    Name string `json:"name"`
    Age  int    `json:"age"`
}


var users = make(map[int]User)
var nextId=1

func createUserHandler(w http.ResponseWriter, r *http.Request) {
  if r.Method != "POST"{
   w.WriteHeader(405)
   return
  }

  var user User
  json.NewDecoder(r.Body).Decode(&user)
  user.ID=nextId
  nextId++

  users[user.ID]=user

  json.NewEncoder(w).Encode(user)
}

func getUserHandler(w http.ResponseWriter, r *http.Request) {
  if r.Method != "GET"{
	w.WriteHeader(405)
	return
  }
  idStr := r.URL.Query().Get("id")
  id,_ := strconv.Atoi(idStr)

  user,ok := users[id]
  if ok == false{
	w.WriteHeader(404)
	return
  }

  json.NewEncoder(w).Encode(user)
}

// func getUserHandler(w http.ResponseWriter, r *http.Request) {
//     if r.Method != http.MethodGet {
//         http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
//         return
//     }
    
//     idStr := r.URL.Query().Get("id")
//     id, err := strconv.Atoi(idStr)
//     if err != nil {
//         http.Error(w, "Invalid ID", http.StatusBadRequest)
//         return
//     }
    
//     user, exists := users[id]
//     if !exists {
//         http.Error(w, "User not found", http.StatusNotFound)
//         return
//     }
    
//     w.Header().Set("Content-Type", "application/json")
//     json.NewEncoder(w).Encode(user)
// }

func userHandler(w http.ResponseWriter, r *http.Request) {

    if r.Method == "POST" {
        createUserHandler(w, r)
        return
    }

    if r.Method == "GET" {
        getUserHandler(w, r)
        return
    }

    w.WriteHeader(405)
}

// func main() {
//     http.HandleFunc("/user", func(w http.ResponseWriter, r *http.Request) {
//         if r.Method == http.MethodPost {
//             createUserHandler(w, r)
//         } else if r.Method == http.MethodGet {
//             getUserHandler(w, r)
//         } else {
//             http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
//         }
//     })
    
//     fmt.Println("Server on :8080")
//     http.ListenAndServe(":8080", nil)
// }

func main(){
	http.HandleFunc("/user",userHandler)
	http.ListenAndServe(":8090",nil)
}