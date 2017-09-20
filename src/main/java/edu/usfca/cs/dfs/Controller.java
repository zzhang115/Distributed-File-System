package edu.usfca.cs.dfs;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;

public class Controller {

    private static ArrayList<StorageNode> storageNodes;
    private static ArrayList<DFSFile> files;
    private static ServerSocket controllerSocket;
    public static void main(String[] args) throws IOException {

        System.out.println("Starting controller...");
        controllerInit();
        while (true) {
            Socket socket = controllerSocket.accept();
            System.out.println("Controller: " + socket.getChannel() + " connected.");
            String line;
            BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            while ((line = reader.readLine()) != null) {
                String receivedRequest = line;
                System.out.println(receivedRequest);
            }
        }
    }

    public static void controllerInit() throws IOException {
        storageNodes = new ArrayList<StorageNode>();
        files = new ArrayList<DFSFile>();
        controllerSocket = new ServerSocket(8080);
    }

}
