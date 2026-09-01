// package main
// import(
// 	"fmt"	
// )

// func main(){
// var queue []string

// queue =append(queue,"hello")
// queue =append(queue,"world")
// queue =append(queue,"test")


// for len(queue) > 0{   
// first := queue[0]
// fmt.Println(first)
// queue=queue[1:]
// }
// }

// // append adds to the back
// // queue[0] reads from the front
// // queue[1:] removes from the front
// // Loop runs until queue is empty



package main

import(
  "fmt"
  "sync" 
  "net/http"
  "encoding/json"
)
type Messege struct{
   Payload string `json:"payload"`
}

type Queue struct{
  messeges []Messege
  mu sync.Mutex
}

// Constructor
func NewQueue() *Queue {
	return &Queue{
		messeges: make([]Messege, 0),
	}
}


func (q *Queue) Enqueue(msg Messege) {
	q.mu.Lock()
	defer q.mu.Unlock()
 q.messeges=append(q.messeges,msg)
}

func (q *Queue) Dequeue() (Messege,bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
   if len(q.messeges) ==0 {
	return Messege{},false
   }

   msg:= q.messeges[0]
   q.messeges=q.messeges[1:]

   return msg,true

}

func (q *Queue) Size() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.messeges)
}

// func main(){
// q := Queue{}
// q.Enqueue("hello")
// q.Enqueue("world")
// q.Enqueue("test")

// fmt.Println(q.Size()) // 3

// msg, ok := q.Dequeue()
// fmt.Println(msg, ok) // hello true

// fmt.Println(q.Size()) // 2
// }

func main(){
	queue := NewQueue()

	// POST /publish - add message
	http.HandleFunc("/publish",func(w http.ResponseWriter,r *http.Request){
		if r.Method != http.MethodPost{
			http.Error(w, "POST only", http.StatusMethodNotAllowed)
			return
		}

		var msg Messege

		if err := json.NewDecoder(r.Body).Decode(&msg); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		if msg.Payload == "" {
         http.Error(w, "payload required", http.StatusBadRequest)
          return
      }
		queue.Enqueue(msg)

		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(map[string]string{
			"status": "enqueued",
		})
    })

	// GET /consume - get message (blocking not yet, just immediate)
	http.HandleFunc("/consume", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "GET only", http.StatusMethodNotAllowed)
			return
		}
		
		msg, ok := queue.Dequeue()
		if !ok {
			w.WriteHeader(http.StatusNoContent) // 204 - empty
			json.NewEncoder(w).Encode(map[string]string{
				"status": "empty",
			})
			return
		}
		
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(msg)
	})

	// GET /size - queue depth
	http.HandleFunc("/size", func(w http.ResponseWriter, r *http.Request) {
		size := queue.Size()
		json.NewEncoder(w).Encode(map[string]int{
			"size": size,
		})
	})

	fmt.Println("Queue on :8080")
	http.ListenAndServe(":8080", nil)
}