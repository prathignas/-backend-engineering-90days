// package main

// import (
//     "fmt"
//     "sync"
// )

// func worker(id int, wg *sync.WaitGroup) {
//     defer wg.Done()
//     fmt.Printf("worker %d starting\n", id)
//     // real work would go here
//     fmt.Printf("worker %d done\n", id)
// }

// func main() {
//     var wg sync.WaitGroup

//     for i := 1; i <= 5; i++ {
//         wg.Add(1)
//         go worker(i, &wg)
//     }

//     wg.Wait()
//     fmt.Println("all workers done")
// }


package main

import (
    "fmt"
    "sync"
)

func main() {
    counter := 0
    var wg sync.WaitGroup
	var mu sync.Mutex

    for i := 0; i < 1000; i++ {
        wg.Add(1)
		
        go func() {
            defer wg.Done()
			mu.Lock()
            counter++
			mu.Unlock()
        }()
    }

    wg.Wait()
    fmt.Println(counter)
}