package edu.usfca.cs.dfs;

import com.google.protobuf.ByteString;

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
            StorageMessages.StorageMessageWrapper msgWrapper
                    = StorageMessages.StorageMessageWrapper.parseDelimitedFrom(
                    socket.getInputStream());

            if (msgWrapper.hasStoreChunkRequestMsg()) {
                StorageMessages.StoreChunkRequest storeChunkRequestMsg
                        = msgWrapper.getStoreChunkRequestMsg();
                System.out.println("Storing file size: "
                        + storeChunkRequestMsg.getFileSize());
            } else if (msgWrapper.hasHeartBeatSignalMsg()) {
                StorageMessages.HeartBeatSignal heartBeatSignalMsg
                        = msgWrapper.getHeartBeatSignalMsg();
                System.out.println("HeartBeat: " + heartBeatSignalMsg.getTimestamp() + " FreeSpace: "
                        + heartBeatSignalMsg.getFreeSpace());
            }
        }
    }

    public static void controllerInit() throws IOException {
        storageNodes = new ArrayList<StorageNode>();
        files = new ArrayList<DFSFile>();
        controllerSocket = new ServerSocket(8080);
    }

}
