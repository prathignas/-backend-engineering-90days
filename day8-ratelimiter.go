package main

import (
	"fmt"
	"net/http"
	"sync"
	"time"
)

// NewRateLimiter(100, 1 minute)
//         ↓
// creates RateLimiter
//         ↓
// creates empty requests map
//         ↓
// stores limit = 100
//         ↓
// stores window = 1 minute
//         ↓
// returns ready-to-use RateLimiter

type RateLimiter struct {
	requests map[string][]time.Time
	mu       sync.Mutex
	limit    int
	window   time.Duration
}

func NewRateLimiter(limit int, window time.Duration) *RateLimiter {
	return &RateLimiter{
		requests: make(map[string][]time.Time),

		limit: limit,

		window: window,
	}

}
func (rl *RateLimiter) Allow(userID string) bool {

	now := time.Now()
	cutoff := now.Add(-rl.window)

	rl.mu.Lock()
	defer rl.mu.Unlock()
	var valid []time.Time
	for _, t := range rl.requests[userID] {
		if t.After(cutoff) {
			valid = append(valid, t)
		}
	}
   rl.requests[userID]=valid

	if len(valid) < rl.limit {
		rl.requests[userID] = append(valid, now)
		return true
	} else {
		return false
	}
}

// HTTP handler (method on struct)
func (rl *RateLimiter) Handler(w http.ResponseWriter, r *http.Request) {
	userID := r.URL.Query().Get("user")

	if userID == "" {
		http.Error(w, "Missing user", http.StatusBadRequest)
		return
	}

	if rl.Allow(userID) {
		w.WriteHeader(http.StatusOK)
		fmt.Fprintln(w, "allow")
	} else {
		w.WriteHeader(http.StatusTooManyRequests)
		fmt.Fprintln(w, "reject")
	}
}

// func main() {
// 	rl := NewRateLimiter(100, time.Minute)
// 	http.HandleFunc("/", rl.Handler)

// 	http.ListenAndServe(":8080", nil)

// }

func main() {
	// Two different rate limiters
	apiLimiter := NewRateLimiter(100, time.Minute)      // 100/min
	loginLimiter := NewRateLimiter(5, time.Minute)      // 5/min (stricter)
	
	http.HandleFunc("/api", apiLimiter.Handler)
	http.HandleFunc("/login", loginLimiter.Handler)
	
	fmt.Println("Server on :8080")
	http.ListenAndServe(":8080", nil)
}