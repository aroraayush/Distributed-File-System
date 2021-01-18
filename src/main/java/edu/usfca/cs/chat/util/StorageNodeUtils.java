package edu.usfca.cs.chat.util;

import java.io.IOException;
import java.nio.file.FileStore;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;

public class StorageNodeUtils {

    public static double getAvailableSpace(){
        double spaceAvailable = 0;
        for (Path root : FileSystems.getDefault().getRootDirectories()) {
            try {
                FileStore store = Files.getFileStore(root);
                double space = (double) store.getUsableSpace()/(1024*1024);
                if(spaceAvailable < space)
                    spaceAvailable = space ; // in mb
            } catch (IOException e) {
                System.out.println("error querying space: " + e.toString());
            }
        }
        return spaceAvailable;
    }
}
