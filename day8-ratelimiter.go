
var count = 0
func  handler(w http.ResponseWriter, r *http.Request) {
  count++

  if count<100 {
	fmt.Fprintln(w,"allow")
  }else{
    fmt.Fprintln(w,"reject")
  }
}
