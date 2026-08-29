var counts = make(map[string]int) // userID -> count

func handler(w http.ResponseWriter, r *http.Request) {
    userID := r.URL.Query().Get("user")
    counts[userID]++
    if counts[userID] > 100 {
        fmt.Fprintln(w, "Blocked")
    } else {
        fmt.Fprintln(w, "Allowed")
    }
}
