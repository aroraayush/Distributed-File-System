package edu.usfca.cs.chat.util;


import java.util.ArrayList;

public class Directory {
    private String name;
    private ArrayList<Directory> children;
    private boolean isDirectory;

    public Directory(String name, boolean isDirectory){
        this.name = name;
        if(isDirectory){
            this.isDirectory = true;
            this.children = new ArrayList<>();
        }
        else{
            this.isDirectory = false;
        }
    }

    public void removeChild(Directory dir,String filename){
        for(Directory child : dir.children){
            if(child.getName().equals(filename) && !child.isDirectory()){
                child = null;
                break;
            }
            else {
                removeChild(child,filename);
            }
        }
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public ArrayList<Directory> getChildren() {
        return isDirectory() ? children : null;
    }

    //    public void setChildren(String fileName) {
//        children[size] = new Directory(0, fileName);
//        this.size++;
//    }

    public boolean isDirectory() {
        return isDirectory;
    }

    public void addChild(String fileName, boolean isDirectory) {
        if(isDirectory()){
            this.children.add(new Directory(fileName,isDirectory));
        }

    }
}
