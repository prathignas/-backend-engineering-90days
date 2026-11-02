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

// Retry settings
const (
    maxAttempts = 5               // after 5 failed tries, give up
    baseDelay   = 1 * time.Second // first wait after a failure; doubles each time
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
	messages []Messege
	mu       sync.Mutex
	file     *os.File
	groups map[string]*ConsumerGroup
	isWorkQueue bool
    offsetFile string
    failWrites  bool                      // NEW, TEST ONLY: true = Enqueue fails on purpose
}

type OutboxMessage struct{
	ID      string   `json:"Id"`   
	Payload string   `json:"payload"` 
	Status string      `json:"status"` 
    NextRetryAt time.Time  `json:"next_retry_at"`          // NEW: don't try again before this
    LastError   string     `json:"last_error,omitempty"`   // NEW: why the last try failed
	CreatedAt time.Time  `json:"created_at"` 
	ProcessedAt *time.Time `json:"processed_at,omitempty"` 
    Attempts int `"json:attempts"`
}

type OutboxStore struct{
	mu   sync.Mutex
	messages []OutboxMessage
	file *os.File
}

type OutboxPoller struct{
    store *OutboxStore
    notifQueue *Queue
    orderQueue *Queue
    stop      chan bool
    stopCleanup chan bool
}

// backoffDelay says how long to wait after the Nth failure.
// failures: 1 → 1s, 2 → 2s, 3 → 4s, 4 → 8s
func backoffDelay(attempts int) time.Duration {
    delay := baseDelay              // start at 1s
    for i := 1; i < attempts; i++ { // double it (attempts - 1) times
        delay = delay * 2
    }
    return delay
}

// saveAllLocked rewrites outbox.log so the file matches memory exactly.
// "Locked" = the caller must already hold o.mu.
func (o *OutboxStore) saveAllLocked() error {
    name := o.file.Name() // "outbox.log"
    tmp := name + ".tmp"  // "outbox.log.tmp"

    // 1. write every message into a brand new temp file
    f, err := os.Create(tmp)
    if err != nil {
        return err
    }
    enc := json.NewEncoder(f)
    for _, m := range o.messages {
        if err := enc.Encode(m); err != nil { // one JSON line per message
            f.Close()
            return err
        }
    }
    f.Sync() // push it onto the disk for real
    f.Close()

    // 2. swap temp file into place — all-or-nothing, same trick as Day 18
    if err := os.Rename(tmp, name); err != nil {
        return err
    }

    // 3. our old file handle still points at the OLD file (now replaced).
    //    Writes to it would be lost. Open the new file so Add() appends to it.
    o.file.Close()
    newFile, err := os.OpenFile(name, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
    if err != nil {
        return err
    }
    o.file = newFile
    return nil
}//NEW: add NextRetryAt and LastError to OutboxMessage , it rewrites the whole file evrytime , if only 1 msg retry changed it rewite the whole file with all msg . 

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
    ticker := time.NewTicker(60 * time.Second)
    go func() {
        for {
            select {
            case <-p.stopCleanup:
                ticker.Stop()
                return
            case <-ticker.C:
                removed := p.store.Cleanup(1 * time.Hour)
                if removed > 0 {
                    fmt.Printf("cleanup removed %d old messages\n", removed)
                }
            }
        }
    }()
}

func (p *OutboxPoller) processPending(){
    ready := p.store.GetReady() // CHANGED: was GetPending()
    for _,msg := range ready{
        queueMsg := Messege{
            ID :msg.ID,
            Payload : msg.Payload,
        }
        if err := p.notifQueue.Enqueue(queueMsg); err != nil {
        p.store.RecordFailure(msg.ID, "notif: "+err.Error())
        continue
      }
        if err := p.orderQueue.Enqueue(queueMsg); err != nil {
        p.store.RecordFailure(msg.ID, "order: "+err.Error())
        continue
     }
     p.store.MarkProcessed(msg.ID);
     fmt.Printf("delivered %s to both queues\n", msg.ID)
    }
}

func (o *OutboxStore) IncrementAttempts(id string) {
    o.mu.Lock()
    defer o.mu.Unlock()

    for i := range o.messages{
      if o.messages[i].ID ==id{
        o.messages[i].Attempts++
        return 
      }
    }
}

func (p *OutboxPoller) Stop() {
    p.stop <- true
    p.stopCleanup<-true
}

func(q *Queue) saveOffsetsLocked() error {
    offsets := make(map[string]int)
    for name, g := range q.groups {
        offsets[name] = g.offset
    }

    data, err := json.Marshal(offsets)
    if err != nil {
        return err
    }

    tmp := q.offsetFile + ".tmp"
    if err := os.WriteFile(tmp, data, 0644); err != nil {
        return err
    }
    return os.Rename(tmp, q.offsetFile)
}

func (q *Queue) loadOffsets() error {
    data, err := os.ReadFile(q.offsetFile)
    if err != nil {
        if os.IsNotExist(err) {
            return nil
        }
        return err
    }

    var offsets map[string]int
    if err := json.Unmarshal(data, &offsets); err != nil {
        return err
    }

    for name, off := range offsets {
        if off > len(q.messages) {
            off = len(q.messages)
        }
        q.groups[name] = &ConsumerGroup{
            name:   name,
            offset: off,
        }
    }
    return nil
}
// Read whole file. If it doesn’t exist — first run, fine, return nil
// Turn JSON back into a map
// For each saved group, create a ConsumerGroup with its offset

// Why the if off > len(q.messages) check? Safety. If the offsets file says 10 but the message log only has 8 (log got cleaned or damaged), offset 10 would crash with index out of range. So we cap it.

// Tag: muscle memory (you did file read + IsNotExist on Day 5)



func (o *OutboxStore) Cleanup(olderThan time.Duration) int {
    o.mu.Lock()
    defer o.mu.Unlock()

    cutoff := time.Now().Add(-olderThan)
    var kept []OutboxMessage
    removed := 0

    for _, msg := range o.messages {
        if msg.Status == "processed" && msg.ProcessedAt != nil && msg.ProcessedAt.Before(cutoff) {
            removed++
            continue
        }
        kept = append(kept, msg)
    }

    o.messages = kept

  // CHANGED: replaces the old close/create/write block.
    // Only touch the disk if something was removed.
    if removed > 0 {
        if err := o.saveAllLocked(); err != nil {
            fmt.Println("cleanup save failed:", err)
        }
    }

    return removed
}

func NewOutboxStore(filename string) (*OutboxStore, error) {
    f, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
    if err != nil {
        return nil, err
    }

    store := &OutboxStore{
        messages: make([]OutboxMessage, 0),
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
            store.messages = append(store.messages, msg)
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
		messages: make([]Messege, 0),
		file:     f,
		groups : make(map[string]*ConsumerGroup),
		isWorkQueue : isWorkQueue,
        offsetFile:  filename + ".offsets", 
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

		q.messages = append(q.messages, msg)

	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
    // Only load offsets for pub/sub queues. Work queues don’t have groups.
    if !isWorkQueue {
        if err := q.loadOffsets(); err != nil {
            return nil, err
        }
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

	o.messages=append(o.messages,msg);
	return msg,nil
	}

func (o *OutboxStore) MarkProcessed(id string) {
    o.mu.Lock()
    defer o.mu.Unlock()

    now := time.Now()
    for i := range o.messages {
        if o.messages[i].ID == id {
            o.messages[i].Status = "processed"
            o.messages[i].ProcessedAt = &now


             // NEW: save to disk, so a restart doesn't deliver it again
             if err := o.saveAllLocked(); err != nil {
                fmt.Println("outbox save failed:", err)
            }
            return

        }
    }
}

// RecordFailure: poller calls this when delivery fails.
// It counts the failure, then decides: wait and retry, or give up.
func (o *OutboxStore) RecordFailure(id string, reason string) {
    o.mu.Lock()
    defer o.mu.Unlock()

    for i := range o.messages {
        if o.messages[i].ID != id {
            continue // not this one, keep looking
        }

        m := &o.messages[i] // m points at the REAL message — changing m changes the slice
        m.Attempts++        // one more failure
        m.LastError = reason

        if m.Attempts >= maxAttempts {
            m.Status = "failed" // give up. Day 20 moves these to the DLQ
            fmt.Printf("%s: failed %d times, giving up\n", m.ID, m.Attempts)
        } else {
            wait := backoffDelay(m.Attempts)
            m.NextRetryAt = time.Now().Add(wait) // poller skips it until then
            fmt.Printf("%s: failure %d/%d, retry in %v\n", m.ID, m.Attempts, maxAttempts, wait)
        }

        if err := o.saveAllLocked(); err != nil { // retry info must survive restart
            fmt.Println("outbox save failed:", err)
        }
        return
    }
}

// Poller sends msg
//    │
//    ├── ✅ success → MarkProcessed  → Status = processed        → save
//    │
//    └── ❌ failure → RecordFailure
//                       ├── Attempts < max → Attempts++, NextRetryAt → save
//                       └── Attempts ≥ max → Attempts++, Status = failed → save



// GetReady: for the POLLER. Messages to try right now:
// status is "pending" AND their wait time is over.
func (o *OutboxStore) GetReady() []OutboxMessage {
    o.mu.Lock()
    defer o.mu.Unlock()

    now := time.Now()
    var ready []OutboxMessage
    for _, m := range o.messages { // a copy is fine here — we only read
        if m.Status != "pending" {
            continue // delivered or given up
        }
        if m.NextRetryAt.After(now) {
            continue // still waiting — too early
        }
        ready = append(ready, m)
    }
    return ready
}

func (o *OutboxStore) GetPending() []OutboxMessage {
    o.mu.Lock()
    defer o.mu.Unlock()

    var pending []OutboxMessage
    for _, m := range o.messages {
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


	if groups.offset >=len(q.messages){
		q.mu.Unlock()
		return Messege{},false
	}

	msg := q.messages[groups.offset]
	groups.offset++

    if err := q.saveOffsetsLocked(); err != nil {
        fmt.Println("save offsets failed:", err)
    }

	q.mu.Unlock()
	return msg,true
} 

func (q *Queue)  consumeWorkQueueInternal() (Messege , bool){
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.messages)==0 {
		return Messege{},false
	}
	msg := q.messages[0]
	q.messages=q.messages[1:]

	q.file.Close()

	newFile,err := os.Create(q.file.Name())
	if err != nil {
		return Messege{},false
	}
	for _,m := range q.messages{
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

// SetFailWrites flips the test switch. TEST ONLY.
func (q *Queue) SetFailWrites(on bool) {
    q.mu.Lock() // poller reads failWrites from another goroutine → lock
    defer q.mu.Unlock()
    q.failWrites = on
}

func (q *Queue) Enqueue(msg Messege) error {
	q.mu.Lock()
	defer q.mu.Unlock()

    if q.failWrites { // NEW, TEST ONLY: pretend this queue is down
        return fmt.Errorf("simulated failure")
    }

	err := json.NewEncoder(q.file).Encode(msg)
	if err != nil {
		return err
	}

	err = q.file.Sync()
	if err != nil {
		return err
	}
	q.messages = append(q.messages, msg)

	return nil
}

func (q *Queue) Dequeue() (Messege, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.messages) == 0 {
		return Messege{}, false
	}

	msg := q.messages[0]
	q.messages = q.messages[1:]

	return msg, true

}

func (q *Queue) Size() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.messages)
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
    }) // its a fan out the wall is half right 

    http.HandleFunc("/notify/offsets", func(w http.ResponseWriter, r *http.Request) {
        notifQueue.mu.Lock()
        offsets := make(map[string]int)
        for name, g := range notifQueue.groups {
            offsets[name] = g.offset
        }
        notifQueue.mu.Unlock()
    
        json.NewEncoder(w).Encode(offsets)
    })//Lets you see where every group is.

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
    all := make([]OutboxMessage, len(outbox.messages))
    copy(all, outbox.messages)
    outbox.mu.Unlock()

    w.WriteHeader(http.StatusOK)
    json.NewEncoder(w).Encode(all)
})

// TEST ONLY: break or fix a queue on purpose.
//   /debug/break?queue=notif&on=true   → notif queue starts failing
//   /debug/break?queue=notif&on=false  → back to normal
http.HandleFunc("/debug/break", func(w http.ResponseWriter, r *http.Request) {
    name := r.URL.Query().Get("queue")
    on := r.URL.Query().Get("on") == "true"

    switch name {
    case "notif":
        notifQueue.SetFailWrites(on)
    case "order":
        orderQueue.SetFailWrites(on)
    default:
        http.Error(w, "queue must be notif or order", http.StatusBadRequest)
        return
    }
    fmt.Fprintf(w, "%s broken = %v\n", name, on)
})

	    fmt.Println("Server on :8080")
    http.ListenAndServe(":8080", nil)
}
