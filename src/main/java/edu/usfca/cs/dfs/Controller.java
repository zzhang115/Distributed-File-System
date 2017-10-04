package edu.usfca.cs.dfs;

import com.google.protobuf.ByteString;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class Controller {

    private static Map<String, Double> storageNodeMap; // <storageNodeHostName, freeSpace>
    private static Map<String, Map<Integer, Set<String>>> metaMap; // <fileName, <chunkId, <storageHostName>>>
    private static Map<String, String> heartBeatMap;  // <storageNodeHostName, timeStamp>
    private static ServerSocket controllerSocket;
    private static final int FAILURE_NODE_TIME = 15;
    private static final int MILLIS_PER_SEC= 1000;
    private static DateFormat dateFormat;

    public static void controllerInit() throws IOException {
        storageNodeMap = new HashMap<String, Double>();
        metaMap = new HashMap<String, Map<Integer, Set<String>>>();
        heartBeatMap = new HashMap<String, String>();
        controllerSocket = new ServerSocket(8080);
        dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");

        Runnable failureDetect = new Runnable() {
            public void run() {
                try {
                    System.out.println("Detecting Failure Node...");
                    detectFailureNode();
                } catch (ParseException e) {
                    e.printStackTrace();
                }
            }
        };

        ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
        executor.scheduleAtFixedRate(failureDetect, 0, 5, TimeUnit.SECONDS);
    }

    public static void main(String[] args) throws IOException {

        System.out.println("Starting controller...");
        controllerInit();
        while (true) {
            Socket socket = controllerSocket.accept();
            ControllerMessages.ControllerMessageWrapper msgWrapper
                    = ControllerMessages.ControllerMessageWrapper.parseDelimitedFrom(
                    socket.getInputStream());

            if (msgWrapper.hasStoreChunkRequestMsg()) {
                ControllerMessages.StoreChunkRequest storeChunkRequestMsg
                        = msgWrapper.getStoreChunkRequestMsg();
                System.out.println("Storing file size: "
                        + storeChunkRequestMsg.getFileSize());
            }
            if (msgWrapper.hasHeartBeatSignalMsg()) {
                ControllerMessages.HeartBeatSignal heartBeatSignalMsg
                        = msgWrapper.getHeartBeatSignalMsg();

                String storageHostName = socket.getInetAddress().getHostName();
                System.out.println("size: " + heartBeatSignalMsg.getMetaCount());
                for (int i = 0; i < heartBeatSignalMsg.getMetaCount(); i++) {
                    String fileName = heartBeatSignalMsg.getMeta(i).getFilename();
                    int chunkId = heartBeatSignalMsg.getMeta(i).getChunkId();
                    System.out.println("i = " + i + "fileName: " + fileName + "chunkId: " + chunkId);
                }
//                System.out.println("HeartBeat: " + heartBeatSignalMsg.getMetaData() + " "
//                        + heartBeatSignalMsg.getTimestamp() + " FreeSpace: " + heartBeatSignalMsg.getFreeSpace());
//                storeInfoFromHeartBeat(storageHostName, heartBeatSignalMsg.getMetaData(),
//                        heartBeatSignalMsg.getFreeSpace(), heartBeatSignalMsg.getTimestamp());
            }
        }
    }

    public static void storeInfoFromHeartBeat
            (String storageHostName, String metaData, double freeSpace, String timeStamp) {
        storageNodeMap.put(storageHostName, freeSpace);
        heartBeatMap.put(storageHostName, timeStamp);
        if (!metaData.equals("")) {
            String fileName = metaData.split(":")[0];
            String[] chunkIdStrs = metaData.split(":")[1].split(",");

            if (!metaMap.containsKey(fileName)) {
                Map<Integer, Set<String>> chunkMap = new HashMap<Integer, Set<String>>();
                metaMap.put(fileName, chunkMap);
            }

            Map<Integer, Set<String>> chunkMap = metaMap.get(fileName);
            for (String chunkIdStr : chunkIdStrs) {
                int chunkId = Integer.parseInt(chunkIdStr);
                if (chunkMap.containsKey(chunkId)) {
                    chunkMap.get(chunkId).add(storageHostName);
                } else {
                    Set<String> storageHostNames = new HashSet<String>();
                    storageHostNames.add(storageHostName);
                    chunkMap.put(chunkId, storageHostNames);
                }
            }
        }
    }

    public static void detectFailureNode() throws ParseException {
        Date currentDate = new Date();

        for (String storageNodeHostName : heartBeatMap.keySet()) {
            Date storageNodeDate = dateFormat.parse(heartBeatMap.get(storageNodeHostName));
            long dateDiff = (currentDate.getTime() - storageNodeDate.getTime()) / MILLIS_PER_SEC;
            System.out.println(dateDiff + " " + currentDate.getTime() +" "+ storageNodeDate.getTime());
            if (dateDiff >= FAILURE_NODE_TIME) {
                System.out.println(storageNodeHostName + " crashed down!");
            }
        }
    }
}
