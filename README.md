Detailed Descriptiom + Design Decesions: [here](https://ayusharora.me/blogs/distributed-systems/storage/building-distributed-file-system.html)

---

# POSIX DFS With Probabilistic Routing
#### Starting with the client
```
git checkout master
java -jar client.jar <Controller_Host> <Controller_Port>
```
```
git checkout master
java -jar client.jar 10.10.12.17 37500
```
#### Using the client
```
======================================
Enter your choice : 
1-Write a File
2-Read a File
3-Show Node List
4-Exit
ls - Listing directories from the file system tree
```
- Enter your choice here

---

###### For writing
- Enter 1
```
Enter the file path for the files you want to write :
```
- Enter the file path and press Enter/Return



###### For reading
- Enter 2
```
Enter the file Name you want to fetch :
```
- Enter the file name and press Enter/Return
```
> The merged file's path will be displayed at the end
