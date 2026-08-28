package main

import (
    "fmt"
    "sync"
)

func process(id int, jobs <-chan int, results chan<- int, wg *sync.WaitGroup) {
    defer wg.Done()
    for j := range jobs {
        result := j * j  // simulate work
        results <- result
    }
}

func main() {
    jobs := make(chan int,10)
    results := make(chan int,10)
    var wg sync.WaitGroup

    // launch 3 workers
    for w := 1; w <= 1; w++ {
        wg.Add(1)
        go process(w, jobs, results, &wg)
    }

    // send 9 jobs
    for k := 1; k <= 10; k++ {
        jobs <- k
    }
    close(jobs)  // no more jobs — workers will exit range loop

    // wait for all workers, then close results
    go func() {
        wg.Wait()
        close(results)
    }()

    // collect results
    for r := range results {
        fmt.Println(r)
    }
}