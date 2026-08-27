
package main

import(
	"path/filepath"
	"testing"
)


func TestSaveAndLoad(t *testing.T) {
   tempDir := t.TempDir()
   tempFile := filepath.Join(tempDir ,"user.json")

   users = map[int]User{
	 1: {ID: 1, Name: "Test", Age: 25},   
   }
    
   err := saveUsers(tempFile)
   if err != nil{
	t.Fatalf("not saved %v",err);
   }

   users=make(map[int]User)

   err = loadUsers(tempFile)
    if err != nil{
		t.Fatalf("cant load %v",err)
	}

	 // Verify  
    if len(users) != 1 {  
        t.Errorf("Expected 1 user, got %d", len(users))  
    }

	user,exists := users[1]
	if !exists {
		t.Fatal("no")
	}

	if user.Name!= "Test" {
		 t.Errorf("Expected name 'Test', got '%s'", user.Name)  
    } 
}


func TestLoadNo(t *testing.T) {
   tempDir := t.TempDir()
   tempFile := filepath.Join(tempDir ,"file not exist")

   users= make(map[int]User)
   
   err := loadUsers(tempFile)
   if err != nil{
	t.Errorf("Loading non-existent file should not error, got: %v", err)
    }
   
	if len(users) != 0 {
	  t.Errorf("Expected empty map, got %d users", len(users))
	}
}



func TestCreateUser(t *testing.T) {
    tempDir := t.TempDir()
    tempFile := filepath.Join(tempDir, "users.json")

    // Reset global state
    users = make(map[int]User)
    var nextID = 1

    // Create user manually (simulate handler)
    user := User{
        ID:   nextID,
        Name: "Alice",
        Age:  30,
    }

    nextID++
    users[user.ID] = user

    // Save
    err := saveUsers(tempFile)
    if err != nil {
        t.Fatal(err)
    }

    // Clear and reload
    users = make(map[int]User)

    err = loadUsers(tempFile)
    if err != nil {
        t.Fatal(err)
    }

    // Verify
    if len(users) != 1 {
        t.Fatal("User not saved")
    }

    if users[1].Age != 30 {
        t.Errorf("Age mismatch")
    }
}