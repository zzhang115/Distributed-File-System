package edu.usfca.cs.dfs;


import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
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
    private static volatile List<STNode> storageNodeList;  //storageNodeQueue sort STNode basic on their free space
    private static Map<String, Map<Integer, Set<String>>> metaMap; // <fileName, <chunkId, <storageHostName>>>
    private static Map<String, String> heartBeatMap;  // <storageNodeHostName, timeStamp>
    private static ServerSocket controllerSocket;
    private static DateFormat dateFormat;
    private static final int FAILURE_NODE_TIME = 10;
    private static final int MILLIS_PER_SEC = 1000;
    private static final int CONTROLLER_PORT = 40000;
    private static Random rand = new Random();

    private static class STNode {
        String storageNodeHostName;
        Double freeSpace;

        STNode(String storageNodeHostName, double freeSpace) {
            this.storageNodeHostName = storageNodeHostName;
            this.freeSpace = freeSpace;
        }

        @Override
        public boolean equals(Object obj) {
            final STNode stNode = (STNode) obj;
            return this.storageNodeHostName.equals(stNode.storageNodeHostName);
        }
    }

    public static void main(String[] args) throws IOException {
//        logger.info("Controller: LocalHostName is " + getHostname());
        controllerInit();
        while (true) {
            handleMessage();
        }
    }

    public static void controllerInit() throws IOException {
        System.setProperty("java.util.logging.SimpleFormatter.format",
                "%5$s%6$s -- %1$tF %1$tT %4$s %2$s%n");
        logger.info("Controller: Initializing...");

        storageNodeList= new ArrayList<STNode>();
        metaMap = new HashMap<String, Map<Integer, Set<String>>>();
        heartBeatMap = new HashMap<String, String>();
        controllerSocket = new ServerSocket(CONTROLLER_PORT);
        dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");

        Runnable failureDetect = new Runnable() {
            public void run() {
                try {
                    logger.info("Controller: Detecting Failure Node...");
                    detectFailureNode();
                } catch (ParseException e) {
                    e.printStackTrace();
                }
            }
        };

        ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
        executor.scheduleAtFixedRate(failureDetect, 0, 2, TimeUnit.SECONDS);
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

            String storageHostName = socket.getInetAddress().getCanonicalHostName();
            logger.info("Controller: Received HeartBeat From " + storageHostName +
                    " FreeSpace: " + heartBeatSignalMsg.getFreeSpace());

            for (int i = 0; i < heartBeatSignalMsg.getMetaCount(); i++) {
                String fileName = heartBeatSignalMsg.getMeta(i).getFilename();
                int chunkId = heartBeatSignalMsg.getMeta(i).getChunkId();
                logger.info("Controller: HeartBeat" +
                        " New FileName: " + fileName + " ChunkId: " + chunkId);
                storeFileInfo(storageHostName, fileName, chunkId);
            }
            storeStorageNodeInfo(storageHostName, heartBeatSignalMsg.getFreeSpace(),
                    heartBeatSignalMsg.getTimestamp());
        }

        if (msgWrapper.hasRetrieveFileMsg()) {
            logger.info("Controller: Received Retrieve File Request");
            ControllerMessages.RetrieveFileRequest retrieveFileMsg =
                    msgWrapper.getRetrieveFileMsg();

            String retrieveFileName = retrieveFileMsg.getFilename();
            logger.info("Controller: Received Retrieve File Request: " + retrieveFileName);
            sendReplyForRetrieving(socket, retrieveFileName);
        }

        if (msgWrapper.hasGetFileListMsg()) {
            ControllerMessages.GetFileListRequest getFileListMsg =
                    msgWrapper.getGetFileListMsg();
            boolean isGetFileList = getFileListMsg.getIsGet();
            if (isGetFileList) {
                logger.info("Controller: Received Get File List Request");

            }
        }
        socket.close();
    }

    public static void sendReplyForGetFileList(Socket socket) {
        ClientMessages.DFSFileList.Builder dfsFileListMsg =
                ClientMessages.DFSFileList.newBuilder();
        for (String fileName : metaMap.keySet()) {
            dfsFileListMsg.addDfsFileBuilder().setFileName(fileName).build();
            for (int chunkId : ((Map<Integer, Set<String>>) metaMap.values()).keySet()) {

            }
            dfsFileListMsg.addDfsFileBuilder().setFileName(fileName)
                    .addDfsChunkBuilder().setChunkId()
        }
    }

    public static void sendReplyForRetrieving(Socket socket, String retrieveFileName) throws IOException {
        logger.info("Controller: Sending Reply For Retrievng File Request: " + retrieveFileName);
        ClientMessages.ReplyForRetrieving.Builder retrieveFileMsg =
                ClientMessages.ReplyForRetrieving.newBuilder();
        if (metaMap.containsKey(retrieveFileName)) {
            Map<Integer, Set<String>> chunkMap = metaMap.get(retrieveFileName);

            for (int chunkId : chunkMap.keySet()) {
                int i = 0;
                int n = rand.nextInt(chunkMap.get(chunkId).size()) + 0;
                for (String storageHostName : chunkMap.get(chunkId)) {
                    // need to add if failure Node
                    if (i == n) {
                        retrieveFileMsg.addRetrieveFileInfoBuilder().setChunkId(chunkId)
                                .setStorageNodeHostName(storageHostName);
                        logger.info("Controller: Choose " + storageHostName +" For Chunk" + chunkId);
                        break;
                    }
                    i++;
                }
            }
            ClientMessages.ClientMessageWrapper msgWrapper =
                    ClientMessages.ClientMessageWrapper.newBuilder()
                            .setReplyForRetrievingMsg(retrieveFileMsg)
                            .build();
            msgWrapper.writeDelimitedTo(socket.getOutputStream());
        }
        ClientMessages.ClientMessageWrapper msgWrapper =
                ClientMessages.ClientMessageWrapper.newBuilder()
                        .setReplyForRetrievingMsg(retrieveFileMsg)
                        .build();
        msgWrapper.writeDelimitedTo(socket.getOutputStream());
        logger.info("Controller: Finished Reply For Retrievng File Request: " + retrieveFileName);
    }

    public static void sendReplyForStoring(Socket socket, double chunkSize) throws IOException {
        Collections.shuffle(storageNodeList);
        logger.info("Controller: Start Send Reply For Storing To Client");
        ClientMessages.AvailStorageNode.Builder availStorageNodeMsg =
                ClientMessages.AvailStorageNode.newBuilder();
        for (STNode stNode : storageNodeList) {
            if (stNode.freeSpace >= chunkSize) {
                logger.info("Controller: Select StorageNode " + stNode.storageNodeHostName);
                availStorageNodeMsg.addStorageNodeHostName(stNode.storageNodeHostName);
            }
        }
        ClientMessages.ClientMessageWrapper msgWrapper =
                ClientMessages.ClientMessageWrapper.newBuilder()
                        .setAvailstorageNodeMsg(availStorageNodeMsg)
                        .build();
        msgWrapper.writeDelimitedTo(socket.getOutputStream());
        logger.info("Controller: Finished Sending Reply For Storing To Client");
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
        STNode stNode = new STNode(storageHostName, freeSpace);
        if (storageNodeList.contains(stNode)) {
            storageNodeList.remove(stNode);
            storageNodeList.add(stNode);
        } else {
            storageNodeList.add(stNode);
        }
        heartBeatMap.put(storageHostName, timeStamp);
    }

    public static void detectFailureNode() throws ParseException {
        Date currentDate = new Date();

        for (int i = 0; i < storageNodeList.size(); i++) {
            STNode stNode = storageNodeList.get(i);
            String storageNodeHostName = stNode.storageNodeHostName;
            Date storageNodeDate = dateFormat.parse(heartBeatMap.get(storageNodeHostName));
            long dateDiff = (currentDate.getTime() - storageNodeDate.getTime()) / MILLIS_PER_SEC;
            if (dateDiff >= FAILURE_NODE_TIME) {
                logger.info("Controller: " + storageNodeHostName + " crashed down!");
                storageNodeList.remove(stNode);
                i--;
            }
        }
    }

    /**
     * Retrieves the short host name of the current host.
     *
     * @return name of the current host
     */
    private static String getHostname()
            throws UnknownHostException {
        return InetAddress.getLocalHost().getHostName();
    }
}
