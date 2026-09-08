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

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"sync"
	"time"
)

type Messege struct {
    ID string `json:"id"`
	Payload string `json:"payload"`
}

type ConsumerGroup struct{
  mu sync.Mutex
  name string
  offset int
}

type Queue struct {
	messeges []Messege
	mu       sync.Mutex
	file     *os.File
	groups map[string]*ConsumerGroup
	isWorkQueue bool
}

// Constructor
func NewQueue(filename string, isWorkQueue bool) (*Queue, error) {
	f, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	q := &Queue{
		messeges: make([]Messege, 0),
		file:     f,
		groups : make(map[string]*ConsumerGroup),
		isWorkQueue : isWorkQueue,
	}

	// Load existing messages from file
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			continue
		}

		var msg Messege
		err := json.Unmarshal([]byte(line), &msg)
		if err != nil {
			return nil, err
		}

		q.messeges = append(q.messeges, msg)

	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return q, nil
}

func (q *Queue) GetOrCreateGroup(name string) *ConsumerGroup{
	q.mu.Lock()
	defer q.mu.Unlock()

	 g ,exists := q.groups[name]
     if exists{
		return g
	 }

	 g = &ConsumerGroup{
		name:name,
		offset:0,
	 }

	 q.groups[name] = g
	 return g
}

func (q *Queue) consumePubSubInternal(groupName string) (Messege, bool) {
	groups := q.GetOrCreateGroup(groupName)

	groups.mu.Lock()
	defer groups.mu.Unlock()

	q.mu.Lock()

	if groups.offset >=len(q.messeges){
		q.mu.Unlock()
		return Messege{},false
	}

	msg := q.messeges[groups.offset]
	groups.offset++

	q.mu.Unlock()
	return msg,true
} 

func (q *Queue)  consumeWorkQueueInternal() (Messege , bool){
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.messeges)==0 {
		return Messege{},false
	}
	msg := q.messeges[0]
	q.messeges=q.messeges[1:]

	q.file.Close()

	newFile,err := os.Create(q.file.Name())
	if err != nil {
		return Messege{},false
	}
	for _,m := range q.messeges{
		json.NewEncoder(newFile).Encode(m)
	}

	q.file = newFile
	

	return msg,true
}

func (q *Queue) Consume(groupName string) (Messege, bool) {
    if q.isWorkQueue {
        return q.consumeWorkQueueInternal()
    } else {
        return q.consumePubSubInternal(groupName)
    }
}

func (q *Queue) Enqueue(msg Messege) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	err := json.NewEncoder(q.file).Encode(msg)
	if err != nil {
		return err
	}

	err = q.file.Sync()
	if err != nil {
		return err
	}
	q.messeges = append(q.messeges, msg)

	return nil
}

func (q *Queue) Dequeue() (Messege, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.messeges) == 0 {
		return Messege{}, false
	}

	msg := q.messeges[0]
	q.messeges = q.messeges[1:]

	return msg, true

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
	// PUB/SUB QUEUE
	    notifQueue, err := NewQueue("notifications.log", false)
           if err != nil {
        fmt.Println("notif queue failed:", err)
        return
    }
    defer notifQueue.file.Close()

	// WORK QUEUE
	    orderQueue, err := NewQueue("orders.log", true)
    if err != nil {
        fmt.Println("order queue failed:", err)
        return
    }
    defer orderQueue.file.Close()

//Notification publish handler (pub/sub)
	    http.HandleFunc("/notify/publish", func(w http.ResponseWriter, r *http.Request) {
        if r.Method != http.MethodPost {
            http.Error(w, "POST only", http.StatusMethodNotAllowed)
            return
        }
        var msg Messege
        if err := json.NewDecoder(r.Body).Decode(&msg); err != nil {
            http.Error(w, "invalid body", http.StatusBadRequest)
            return
        }
        msg.ID = fmt.Sprintf("%d", time.Now().UnixNano())
        if err := notifQueue.Enqueue(msg); err != nil {
            http.Error(w, "failed to persist", http.StatusInternalServerError)
            return
        }
        w.WriteHeader(http.StatusCreated)
        json.NewEncoder(w).Encode(map[string]string{"status": "enqueued"})
    })

	//order work QUEUe
	    http.HandleFunc("/order/publish", func(w http.ResponseWriter, r *http.Request) {
        if r.Method != http.MethodPost {
            http.Error(w, "POST only", http.StatusMethodNotAllowed)
            return
        }
        var msg Messege
        if err := json.NewDecoder(r.Body).Decode(&msg); err != nil {
            http.Error(w, "invalid body", http.StatusBadRequest)
            return
        }
        msg.ID = fmt.Sprintf("%d", time.Now().UnixNano())
        if err := orderQueue.Enqueue(msg); err != nil {
            http.Error(w, "failed to persist", http.StatusInternalServerError)
            return
        }
        w.WriteHeader(http.StatusCreated)
        json.NewEncoder(w).Encode(map[string]string{"status": "enqueued"})
    })

	//Notification subscribe handler (pub/sub read)
	    http.HandleFunc("/notify/subscribe", func(w http.ResponseWriter, r *http.Request) {
        if r.Method != http.MethodGet {
            http.Error(w, "GET only", http.StatusMethodNotAllowed)
            return
        }
        groupName := r.URL.Query().Get("group")
        if groupName == "" {
            groupName = "default"
        }
        msg, ok := notifQueue.Consume(groupName)
        if !ok {
            w.WriteHeader(http.StatusNoContent)
            json.NewEncoder(w).Encode(map[string]string{"status": "empty"})
            return
        }
        w.WriteHeader(http.StatusOK)
        json.NewEncoder(w).Encode(msg)
    })

	//Order work handler (work queue read)
	    http.HandleFunc("/order/work", func(w http.ResponseWriter, r *http.Request) {
        if r.Method != http.MethodGet {
            http.Error(w, "GET only", http.StatusMethodNotAllowed)
            return
        }
        msg, ok := orderQueue.Consume("")
        if !ok {
            w.WriteHeader(http.StatusNoContent)
            json.NewEncoder(w).Encode(map[string]string{"status": "empty"})
            return
        }
        w.WriteHeader(http.StatusOK)
        json.NewEncoder(w).Encode(msg)
    })

// one event goes to both the queue
	    http.HandleFunc("/event/publish", func(w http.ResponseWriter, r *http.Request) {
        if r.Method != http.MethodPost {
            http.Error(w, "POST only", http.StatusMethodNotAllowed)
            return
        }

        var msg Messege
        if err := json.NewDecoder(r.Body).Decode(&msg); err != nil {
            http.Error(w, "invalid body", http.StatusBadRequest)
            return
        }

        msg.ID = fmt.Sprintf("%d", time.Now().UnixNano())

        if err := notifQueue.Enqueue(msg); err != nil {
            http.Error(w, "notif queue failed", http.StatusInternalServerError)
            return
        }

        if err := orderQueue.Enqueue(msg); err != nil {
            http.Error(w, "order queue failed", http.StatusInternalServerError)
            return
        }

        w.WriteHeader(http.StatusCreated)
        json.NewEncoder(w).Encode(map[string]string{
            "status": "published to all queues",
            "id":     msg.ID,
        })
    })

	    fmt.Println("Server on :8080")
    http.ListenAndServe(":8080", nil)
}
