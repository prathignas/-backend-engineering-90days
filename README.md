# -backend-engineering-90days

## Day 1

- Learned Go basics
- Built a calculator


day 6 
----
Lifecycle in this test:

t.TempDir() → /tmp/TestSaveAndLoad.../ created on disk
filepath.Join(...) → just a string, nothing on disk
saveUsers(tempFile) → user.json created on disk, JSON written into it
loadUsers(tempFile) → reads from that same file
Test ends → entire temp directory and the file inside it get deleted automatically
