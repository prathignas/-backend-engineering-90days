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

type OutboxMessage struct{
	ID      string   `json:"Id"`   
	Payload string   `json:"payload"` 
	Status string      `json:"status"` 
	CreatedAt time.Time  `json:"created_at"` 
	ProcessedAt *time.Time `json:"processed_at,omitempty"` 
    Attempts int `"json:attempts"`
}

type OutboxStore struct{
	mu   sync.Mutex
	messeges []OutboxMessage
	file *os.File
}

type OutboxPoller struct{
    store *OutboxStore
    notifQueue *Queue
    orderQueue *Queue
    stop      chan bool
    stopCleanup chan bool
}

func NewOutboxPoller(store *OutboxStore,notifQueue,orderQueue *Queue) *OutboxPoller{
    return &OutboxPoller{
    store:      store,
    notifQueue: notifQueue,
    orderQueue: orderQueue,
    stop:       make(chan bool),
    stopCleanup: make(chan bool),
 }
}

func (p *OutboxPoller) Start(){
    ticker := time.NewTicker(100 * time.Millisecond)
    go func(){
        for{
            select{
            case <- p.stop:
                ticker.Stop()
                return 

            case<-ticker.C:
                p.processPending()
            }
        }
    }()
}

func (p *OutboxPoller) StartCleanup() {
    ticker := time.NewTicker(10 * time.Second)
    go func() {
        for {
            select {
            case <-p.stopCleanup:
                ticker.Stop()
                return
            case <-ticker.C:
                removed := p.store.Cleanup(5 * time.Second)
                if removed > 0 {
                    fmt.Printf("cleanup removed %d old messages\n", removed)
                }
            }
        }
    }()
}

func (p *OutboxPoller) processPending(){
    pending := p.store.GetPending();
    for _,msg := range pending{
        queueMsg := Messege{
            ID :msg.ID,
            Payload : msg.Payload,
        }
        if err := p.notifQueue.Enqueue(queueMsg); err != nil {
        fmt.Printf("notif enqueue failed for %s: %v\n", msg.ID, err)
        p.store.IncrementAttempts(msg.ID)
        continue
      }
        if err := p.orderQueue.Enqueue(queueMsg); err != nil {
        fmt.Printf("order enqueue failed for %s: %v\n", msg.ID, err)
        p.store.IncrementAttempts(msg.ID)
        continue
     }
     p.store.MarkProcessed(msg.ID);
     fmt.Printf("delivered %s to both queues\n", msg.ID)
    }
}

func (o *OutboxStore) IncrementAttempts(id string) {
    o.mu.Lock()
    defer o.mu.Unlock()

    for i := range o.messeges{
      if o.messeges[i].ID ==id{
        o.messeges[i].Attempts++
        return 
      }
    }
}

func (p *OutboxPoller) Stop() {
    p.stop <- true
    p.stopCleanup<-true
}

func (o *OutboxStore) Cleanup(olderThan time.Duration) int {
    o.mu.Lock()
    defer o.mu.Unlock()

    cutoff := time.Now().Add(-olderThan)
    var kept []OutboxMessage
    removed := 0

    for _, msg := range o.messeges {
        if msg.Status == "processed" && msg.ProcessedAt != nil && msg.ProcessedAt.Before(cutoff) {
            removed++
            continue
        }
        kept = append(kept, msg)
    }

    o.messeges = kept

    o.file.Close()
    newFile, err := os.Create(o.file.Name())
    if err != nil {
        return removed
    }
    for _, m := range o.messeges {
        json.NewEncoder(newFile).Encode(m)
    }
    o.file = newFile

    return removed
}

func NewOutboxStore(filename string) (*OutboxStore, error) {
    f, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
    if err != nil {
        return nil, err
    }

    store := &OutboxStore{
        messeges: make([]OutboxMessage, 0),
        file:     f,
    }

    scanner := bufio.NewScanner(f)
    for scanner.Scan() {
        line := scanner.Text()
        if line == "" {
            continue
        }
        var msg OutboxMessage
        if err := json.Unmarshal([]byte(line), &msg); err == nil {
            store.messeges = append(store.messeges, msg)
        }
    }

    return store, nil
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

func(o *OutboxStore) add(payload string) (OutboxMessage,error){
	o.mu.Lock()
	defer o.mu.Unlock()

	msg := OutboxMessage{
		ID:        fmt.Sprintf("%d", time.Now().UnixNano()),
        Payload:   payload,
        Status:    "pending",
        CreatedAt: time.Now(),
    }
	if err := json.NewEncoder(o.file).Encode(msg);  err != nil{
		return OutboxMessage{},err
	}
	o.file.Sync()

	o.messeges=append(o.messeges,msg);
	return msg,nil
	}

func (o *OutboxStore) MarkProcessed(id string) {
    o.mu.Lock()
    defer o.mu.Unlock()

    now := time.Now()
    for i := range o.messeges {
        if o.messeges[i].ID == id {
            o.messeges[i].Status = "processed"
            o.messeges[i].ProcessedAt = &now
            return
        }
    }
}

func (o *OutboxStore) GetPending() []OutboxMessage {
    o.mu.Lock()
    defer o.mu.Unlock()

    var pending []OutboxMessage
    for _, m := range o.messeges {
        if m.Status == "pending" {
            pending = append(pending, m)
        }
    }
    return pending
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


 outbox, err := NewOutboxStore("outbox.log")
if err != nil {
    fmt.Println("outbox failed:", err)
    return
}
defer outbox.file.Close()

poller := NewOutboxPoller(outbox, notifQueue, orderQueue)
poller.Start()
poller.StartCleanup()
defer poller.Stop()


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
	    http.HandleFunc("/event/publish1", func(w http.ResponseWriter, r *http.Request) {
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

    http.HandleFunc("/event/publish", func(w http.ResponseWriter, r *http.Request) {
    if r.Method != http.MethodPost {
        http.Error(w, "POST only", http.StatusMethodNotAllowed)
        return
    }

    var body struct {
        Payload string `json:"payload"`
    }
    if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
        http.Error(w, "invalid body", http.StatusBadRequest)
        return
    }

    msg, err := outbox.add(body.Payload)
    if err != nil {
        http.Error(w, "outbox write failed", http.StatusInternalServerError)
        return
    }

    w.WriteHeader(http.StatusCreated)
    json.NewEncoder(w).Encode(map[string]string{
        "status": "queued in outbox",
        "id":     msg.ID,
    })
})

http.HandleFunc("/outbox/pending", func(w http.ResponseWriter, r *http.Request) {
    pending := outbox.GetPending()
    w.WriteHeader(http.StatusOK)
    json.NewEncoder(w).Encode(pending)
})

http.HandleFunc("/outbox/all", func(w http.ResponseWriter, r *http.Request) {
    outbox.mu.Lock()
    all := make([]OutboxMessage, len(outbox.messeges))
    copy(all, outbox.messeges)
    outbox.mu.Unlock()

    w.WriteHeader(http.StatusOK)
    json.NewEncoder(w).Encode(all)
})

	    fmt.Println("Server on :8080")
    http.ListenAndServe(":8080", nil)
}
