package edu.usfca.cs.dfs;


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
import java.util.logging.Logger;

public class Controller {

    private static Logger logger = Logger.getLogger("Log");
    private static PriorityQueue<STNode> storageNodeQueue;  //storageNodeQueue sort STNode basic on their free space
    private static Map<String, Map<Integer, Set<String>>> metaMap; // <fileName, <chunkId, <storageHostName>>>
    private static Map<String, String> heartBeatMap;  // <storageNodeHostName, timeStamp>
    private static ServerSocket controllerSocket;
    private static DateFormat dateFormat;
    private static final int FAILURE_NODE_TIME = 15;
    private static final int MILLIS_PER_SEC= 1000;
    private static final int COPY_NUM = 3;

    private static class STNode {
        String storageNodeHostName;
        Double freeSpace;
        STNode(String storageNodeHostName, double freeSpace) {
            this.storageNodeHostName = storageNodeHostName;
            this.freeSpace = freeSpace;
        }
    }

    private static class FreeSpaceComparator implements Comparator<STNode> {
        public int compare(STNode a, STNode b) {
            if (a.freeSpace > b.freeSpace) {
                return 1;
            } else if (a.freeSpace < b.freeSpace) {
                return -1;
            } else {
                return 0;
            }
        }
    }

    public static void controllerInit() throws IOException {
        storageNodeQueue = new PriorityQueue<STNode>(new FreeSpaceComparator());
        metaMap = new HashMap<String, Map<Integer, Set<String>>>();
        heartBeatMap = new HashMap<String, String>();
        controllerSocket = new ServerSocket(8080);
        dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");

        Runnable failureDetect = new Runnable() {
            public void run() {
                try {
                    System.out.println("Controller: Detecting Failure Node...");
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
            handleMessage();
        }
    }

    public static void handleMessage() throws IOException {
        Socket socket = controllerSocket.accept();
        ControllerMessages.ControllerMessageWrapper msgWrapper
                = ControllerMessages.ControllerMessageWrapper.parseDelimitedFrom(
                socket.getInputStream());

        if (msgWrapper.hasStoreChunkRequestMsg()) {
            ControllerMessages.StoreChunkRequest storeChunkRequestMsg
                    = msgWrapper.getStoreChunkRequestMsg();
            logger.info("Controller: Received Storing Request, File Size Is "
                    + storeChunkRequestMsg.getFileSize());
            sendReplyForStoring(socket, storeChunkRequestMsg.getFileSize());
        }

        if (msgWrapper.hasHeartBeatSignalMsg()) {
            ControllerMessages.HeartBeatSignal heartBeatSignalMsg =
                    msgWrapper.getHeartBeatSignalMsg();

            String storageHostName = socket.getInetAddress().getHostName();
            for (int i = 0; i < heartBeatSignalMsg.getMetaCount(); i++) {
                String fileName = heartBeatSignalMsg.getMeta(i).getFilename();
                int chunkId = heartBeatSignalMsg.getMeta(i).getChunkId();
                System.out.println("i = " + i + "fileName: " + fileName + "chunkId: " + chunkId);
                storeFileInfo(storageHostName, fileName, chunkId);
            }
            logger.info("Controller: Received HeartBeat: " + heartBeatSignalMsg.getTimestamp()
                    + " FreeSpace: " + heartBeatSignalMsg.getFreeSpace());
            storeStorageNodeInfo(storageHostName, heartBeatSignalMsg.getFreeSpace(),
                    heartBeatSignalMsg.getTimestamp());
        }

        if (msgWrapper.hasRetrieveFileMsg()) {
            ControllerMessages.RetrieveFileRequest retrieveFileMsg =
                    msgWrapper.getRetrieveFileMsg();

            String retrieveFileName = retrieveFileMsg.getFilename();
            logger.info("Controller: Received Retrieve File Request: " + retrieveFileName);
            logger.info("Controller: Sending Reply For Retrievng File Request: " + retrieveFileName);
            sendReplyForRetrieving(socket, retrieveFileName);
        }
    }

    public static void sendReplyForRetrieving(Socket socket, String retrieveFileName) throws IOException {
        if (metaMap.containsKey(retrieveFileName)) {
            Map<Integer, Set<String>> chunkMap = metaMap.get(retrieveFileName);
            ClientMessages.ReplyForRetrieving.Builder retrieveFileMsg =
                    ClientMessages.ReplyForRetrieving.newBuilder();

            for (int chunkId : chunkMap.keySet()) {
                for (String storageHostName : chunkMap.get(chunkId)) {
                    // need to add if failure Node
                    retrieveFileMsg.addRetrieveFileInfoBuilder().setChunkId(chunkId)
                            .setStorageNodeHostName(storageHostName);
                    break;
                }
            }
            ClientMessages.ClientMessageWrapper msgWrapper =
                    ClientMessages.ClientMessageWrapper.newBuilder()
                            .setReplyForRetrievingMsg(retrieveFileMsg)
                            .build();
            msgWrapper.writeDelimitedTo(socket.getOutputStream());
        }
    }

    public static void sendReplyForStoring(Socket socket, double fileSize) throws IOException {
        int nodeNum = Math.min(COPY_NUM, storageNodeQueue.size());
        List<STNode> availNodes = new ArrayList<STNode>();
        ClientMessages.AvailStorageNode.Builder availStorageNodeMsg =
                ClientMessages.AvailStorageNode.newBuilder();
        for (int i = 0; i < nodeNum; i++) {
            STNode stNode = storageNodeQueue.poll();
            if (stNode.freeSpace < fileSize) {
                break;
            }
            availNodes.add(stNode);
            availStorageNodeMsg.addStorageNodeHostName(stNode.storageNodeHostName);
        }
        ClientMessages.ClientMessageWrapper msgWrapper =
                ClientMessages.ClientMessageWrapper.newBuilder()
                    .setAvailstorageNodeMsg(availStorageNodeMsg)
                    .build();
        msgWrapper.writeDelimitedTo(socket.getOutputStream());
        for (STNode stNode : availNodes) {
            storageNodeQueue.offer(stNode);
        }
    }

    public static void storeFileInfo(String storageHostName, String fileName, int chunkId) {
        if (!metaMap.containsKey(fileName)) {
            Map<Integer, Set<String>> chunkMap = new HashMap<Integer, Set<String>>();
            metaMap.put(fileName, chunkMap);
        }

        Map<Integer, Set<String>> chunkMap = metaMap.get(fileName);
        if (chunkMap.containsKey(chunkId)) {
            chunkMap.get(chunkId).add(storageHostName);
        } else {
            Set<String> storageHostNames = new HashSet<String>();
            storageHostNames.add(storageHostName);
            chunkMap.put(chunkId, storageHostNames);
        }
    }

    public static void storeStorageNodeInfo(String storageHostName, double freeSpace, String timeStamp) {
        storageNodeQueue.offer(new STNode(storageHostName, freeSpace));
        heartBeatMap.put(storageHostName, timeStamp);
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
