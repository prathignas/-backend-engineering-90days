 package main

 import(
	 	"fmt"
		//"math"
 )

// func main(){
// 	sum:=1
// 	for sum<1000 {
// 	 sum+= sum
// }
// fmt.Println(sum)
// }


// func pow(x,n,lim float64) float64{
//    if v:= math.Pow(x,n) ; v<lim {
// 	return v
//    }
//    return lim
// }

// func main() {
// 	fmt.Println(
// 		pow(3, 2, 10),
// 		pow(3, 3, 20),
// 	)
// }

//POINTER
// func main(){
// 	i,j := 42,100

// 	p := &i
// 	fmt.Println(*p)
// 	*p=10
// 	fmt.Println(i)

// 	p = &j
// 	fmt.Println(*p)
// 	*p=*p/10
// 	fmt.Println(j)
// }

func add(x,y int) int{
	return x+y
}

func sub(x,y int)int{
	return x-y
}

func mul(x,y int)int{
	return x*y
}

func div(x,y float64) (float64,error){
	if y==0 {
		return 0,fmt.Errorf("connot div by zero")
	} else{
		return x/y,nil
	}
}

func main(){
	fmt.Println(add(5,3))
	fmt.Println(sub(10,4))
	fmt.Println(mul(5,2))
	
	result,err := div(10,0)
	if err!=nil{
	  fmt.Println("Error",err)
	} else{
		fmt.Println(result)
	}
   
	result,err = div(30,6)
	if err!=nil{
	  fmt.Println("Error",err)
	} else{
		fmt.Println(result)
	}
}