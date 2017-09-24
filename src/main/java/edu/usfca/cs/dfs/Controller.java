package edu.usfca.cs.dfs;

import com.google.protobuf.ByteString;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;

public class Controller {

    private static Map<String, Double> storageNodeMap; // <storageNodeHostName, freeSpace>
    private static Map<String, Map<Integer, Set<String>>> metaMap; // <fileName, <chunkId, <storageHostName>>>
    private static Map<String, String> heartBeatMap;  // <storageNodeHostName, timeStamp>
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
            }
            if (msgWrapper.hasHeartBeatSignalMsg()) {
                StorageMessages.HeartBeatSignal heartBeatSignalMsg
                        = msgWrapper.getHeartBeatSignalMsg();

                String storageHostName = socket.getInetAddress().getHostName();
                System.out.println("HeartBeat: " + heartBeatSignalMsg.getMetaData() + " "
                        + heartBeatSignalMsg.getTimestamp() + " FreeSpace: " + heartBeatSignalMsg.getFreeSpace());

            }
        }
    }

    public static void storeInfoFromHeartBeat
            (String storageHostName, String metaData, double freeSpace, String timeStamp) {
        storageNodeMap.put(storageHostName, freeSpace);
        heartBeatMap.put(storageHostName, timeStamp);
        String fileName = metaData.split(":")[0];
        String[] chunkIdStrs = metaData.split(":")[1].split(",");

        if (!metaMap.containsKey(fileName)) {
            Map<Integer, Set<String>> chunkMap = new HashMap<Integer, Set<String>>();
            metaMap.put(fileName, chunkMap);
        }

        Map<Integer, Set<String>> chunkMap = metaMap.get(fileName);
        for (String chunkIdStr : chunkIdStrs) {
            int chunkId= Integer.parseInt(chunkIdStr);
            if (chunkMap.containsKey(chunkId)) {
                chunkMap.get(chunkId).add(storageHostName);
            } else {
                Set<String> storageHostNames = new HashSet<String>();
                storageHostNames.add(storageHostName);
                chunkMap.put(chunkId, storageHostNames);
            }
        }

    }

    public static void controllerInit() throws IOException {
        storageNodeMap = new HashMap<String, Double>();
        metaMap = new HashMap<String, Map<Integer, Set<String>>>();
        heartBeatMap = new HashMap<String, String>();
        controllerSocket = new ServerSocket(8080);
    }

}
