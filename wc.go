// package main

// import(
// 	"fmt"
// 	"strings"
// )

// func main (){
// 	line:= " hello go hello"

// 	words:= strings.Fields(line)

// 	for _,word:=range words{
// 		fmt.Println(word)
// 	}
// }



// COUNTS SPECIFIC WORD USING FOR LOOP



// // COUNT ONE WORD
// package main

// import(
// 	"fmt"
// 	"strings"
// )

// func main(){
// 	line:="hello go hello"

// 	words := strings.Fields(line)

// 	counts:=0

// 	for _,word :=range words{
// 		if word=="hello"{
// 		counts++
// 		}
// 	}
// 	fmt.Println("word apprears",counts)
// }


//.    ADDING MAP AND COUNT EACH WORD FREQUENCY

// package main

// import(
// 	"fmt"
// 	"strings"

// )

// func main(){
// 	line:= "hello go hello"

// 	words := strings.Fields(line)

// 	counts := make(map[string]int)

// 	for _,word := range words{
// 		counts[word]++
// 	}

// 	for word,count := range counts{
// 		fmt.Printf("%s %d\n",word,count)
// 	}
// }

  
// WORD COUNT PROGRAM
package main

import(
	"fmt"
	"strings"
	"os"
	"sort"
)

type WordCount struct{
	Word string
	Count int
}

func wordcount(s string) []WordCount{
    words := strings.Fields(s)

	counts := make(map[string]int)

	for _,word:=range words{
		word =strings.ToLower(word)
		counts[word]++
	}

	var wordcounts []WordCount
	for word,count := range counts{
         wc := WordCount{
          Word:word,
		  Count:count,
		 }
		 wordcounts=append(wordcounts,wc)
	}

	sort.Slice(wordcounts,func(i,j int)bool{
		return wordcounts[i].Count > wordcounts[j].Count
	})
	return wordcounts
}

func main(){
	data ,err := os.ReadFile("sample.txt")

	if err != nil{
		fmt.Println(err)
		return
	}

	result:=wordcount(string(data))
	
	for i:=0 ; i<5 && i<len(result);i++ {
		fmt.Printf("%s %d\n",result[i].Word,result[i].Count)
	}
	
}