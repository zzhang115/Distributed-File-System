package edu.usfca.cs.dfs;

import java.util.ArrayList;

public class Controller {

    private static ArrayList<StorageNode> storageNodes;
    private static ArrayList<DFSFile> files;
    public static void main(String[] args) {

        System.out.println("Starting controller...");


    }

    public static void controllerInit() {
        storageNodes = new ArrayList<StorageNode>();
        files = new ArrayList<DFSFile>();
    }

}
