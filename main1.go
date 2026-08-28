// package main

// import (
// 	"fmt"
// 	"time"
// )

// func say(s string){
// 	for i:=0;i<3;i++ {
// 		time.Sleep(100*time.Millisecond)
//       fmt.Println(s)
// 	}
// }

// func main() {
// 	go say("goroutine")
//     say("main")
// }


// package main

// import(
// 	"fmt"
// )

// func sum(s []int,ch chan int){
// 	total:=0

// 	for _,v := range s{
//        total += v
// 	}
// 	ch<-total
// }

// func main(){
// 	s := []int{7,2,4,-9,4,6}

// 	ch := make(chan int)
// 	go sum(s[:len(s)/2],ch)
// 	go sum(s[len(s)/2:],ch)


// 	x,y := <-ch,<-ch
// 	fmt.Println(x,y,x+y)
// }




//////// Close and range

// package main
// import(
// 	"fmt"
// )

// func fib(n int,ch chan int){
// 	x ,y := 0,1
// 	for i:=1;i<=n;i++{
// 	ch<-x
// 	x,y = y,x+y
// 	}

// 	close(ch)
// }

// func main(){
// 	ch := make(chan int,10)
// 	go fib(cap(ch),ch)

// 	for v:= range ch{
// 		fmt.Println(v)
// 	}
// }


//////// select 

package main

import (
    "fmt"
    "time"
)

func main() {
    ch1 := make(chan string)
    ch2 := make(chan string)

    go func() {
        time.Sleep(1 * time.Second)
        ch1 <- "one"
		time.Sleep(1 * time.Second)
		ch1<-"three"
    }()

    go func() {
        time.Sleep(6 * time.Second)
        ch2 <- "two"
    }()

    for i := 0; i < 4; i++ {
        select {
        case msg1 := <-ch1:
            fmt.Println("received", msg1)
        case msg2 := <-ch2:
            fmt.Println("received", msg2)
		case <-time.After(2 * time.Second):
            fmt.Println("timeout")
        }
    }
}