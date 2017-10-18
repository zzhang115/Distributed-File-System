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
    private static volatile Map<String, Map<Integer, Set<String>>> metaMap; // <fileName, <chunkId, <storageHostName>>>
    private static volatile Map<String, List<String>> hostMetaMap; // <hostName, <fileName_chunkId>>
    private static volatile Map<String, String> heartBeatMap;  // <storageNodeHostName, timeStamp>
    private static ServerSocket controllerSocket;
    private static DateFormat dateFormat;
    private static final int FAILURE_NODE_TIME = 10;
    private static final int MILLIS_PER_SEC = 1000;
    private static final int CONTROLLER_PORT = 40000;
    private static final int STORAGENODE_PORT = 40010;
    private static final int REPLY_WAITING_TIME = 10000;
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

    public static void main(String[] args) throws IOException, InterruptedException {
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
        hostMetaMap = new HashMap<String, List<String>>();
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
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        };

        ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
        executor.scheduleAtFixedRate(failureDetect, 0, 5, TimeUnit.SECONDS);
    }

    public static void handleMessage() throws IOException, InterruptedException {
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
            if (!hostMetaMap.keySet().contains(storageHostName)) {
                List<String> fileAndChunk = new ArrayList<String>();
                hostMetaMap.put(storageHostName, fileAndChunk);
            }
            if (!heartBeatMap.keySet().contains(storageHostName)) {
                sendRetrieveMetaRequest(storageHostName);
            }
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
                sendReplyForGetFileList(socket);
            }
        }
        socket.close();
    }

    public static void sendReplyForGetFileList(Socket socket) throws IOException {
        logger.info("Controller: Start Send Get File List");
        ClientMessages.DFSFileList.Builder dfsFileListMsg =
                ClientMessages.DFSFileList.newBuilder();
        for (String fileName : metaMap.keySet()) {
            ClientMessages.DFSFile.Builder dfsFileMsg = ClientMessages.DFSFile.newBuilder();
            dfsFileMsg.setFileName(fileName);
            Map<Integer, Set<String>> chunkMap = metaMap.get(fileName);
            for (int chunkId : chunkMap.keySet()) {
                ClientMessages.DFSChunk.Builder dfsChunkMsg = ClientMessages.DFSChunk.newBuilder();
                dfsChunkMsg.setChunkId(chunkId);
                Set<String> storageNodeHostNames = chunkMap.get(chunkId);
                for (String storageNodeHostName : storageNodeHostNames) {
                    if (heartBeatMap.keySet().contains(storageNodeHostName)) {
                        dfsChunkMsg.addStorageNodeHostName(storageNodeHostName);
                    }
                }
                dfsFileMsg.addDfsChunk(dfsChunkMsg);
            }
            dfsFileListMsg.addDfsFile(dfsFileMsg);
        }

        ClientMessages.ClientMessageWrapper msgWrapper =
                ClientMessages.ClientMessageWrapper.newBuilder()
                        .setDfsFileListMsg(dfsFileListMsg)
                        .build();
        msgWrapper.writeDelimitedTo(socket.getOutputStream());
        logger.info("Controller: Finished Send Get File List");
    }

    public static void sendReplyForRetrieving(Socket socket, String retrieveFileName) throws IOException {
        logger.info("Controller: Sending Reply For Retrievng File Request: " + retrieveFileName);
        ClientMessages.ReplyForRetrieving.Builder retrieveFileMsg =
                ClientMessages.ReplyForRetrieving.newBuilder();
        if (metaMap.containsKey(retrieveFileName)) {
            Map<Integer, Set<String>> chunkMap = metaMap.get(retrieveFileName);

            for (int chunkId : chunkMap.keySet()) {
//                int i = 0;
//                int n = rand.nextInt(chunkMap.get(chunkId).size()) + 0;
                ClientMessages.RetrieveFileInfo.Builder retrieveFileInfoMsg =
                        ClientMessages.RetrieveFileInfo.newBuilder();

//                retrieveFileMsg.addRetrieveFileInfoBuilder().setChunkId(chunkId);
                retrieveFileInfoMsg.setChunkId(chunkId);
                for (String storageHostName : chunkMap.get(chunkId)) {
//                    if (i == n) {
//                        retrieveFileMsg.addRetrieveFileInfoBuilder().setChunkId(chunkId)
//                                .setStorageNodeHostName(storageHostName);
                        retrieveFileInfoMsg.addStorageNodeHostName(storageHostName);
                        logger.info("Controller: Choose " + storageHostName +" For Chunk" + chunkId);
//                        break;
//                    }
//                    i++;
                }
                retrieveFileMsg.addRetrieveFileInfo(retrieveFileInfoMsg);
            }
            ClientMessages.ClientMessageWrapper msgWrapper =
                    ClientMessages.ClientMessageWrapper.newBuilder()
                            .setReplyForRetrievingMsg(retrieveFileMsg)
                            .build();
            msgWrapper.writeDelimitedTo(socket.getOutputStream());
            logger.info("Controller: Finished Reply For Retrievng File Request: " + retrieveFileName);
        } else {
            logger.info("Controller: No Such File In This DFS");
        }
//        ClientMessages.ClientMessageWrapper msgWrapper =
//                ClientMessages.ClientMessageWrapper.newBuilder()
//                        .setReplyForRetrievingMsg(retrieveFileMsg)
//                        .build();
//        msgWrapper.writeDelimitedTo(socket.getOutputStream());
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

    public static void sendRetrieveMetaRequest(String storageHostName) throws IOException, InterruptedException {
        logger.info("Controller: Start Send Retrieve Meta Request To StorageNode");
        Socket storageNodeSocket = new Socket(storageHostName, STORAGENODE_PORT);

        StorageMessages.RetrieveMeta.Builder retrieveMetaMsg =
                StorageMessages.RetrieveMeta.newBuilder();
        retrieveMetaMsg.setIsRetrieveMeta(true);

        StorageMessages.StorageMessageWrapper msgWrapper = StorageMessages
                .StorageMessageWrapper.newBuilder()
                .setRetrieveMetaMsg(retrieveMetaMsg)
                .build();
        msgWrapper.writeDelimitedTo(storageNodeSocket.getOutputStream());
        logger.info("Controller: Finished Send Retrieve Meta Request To StorageNode");
        getFullMetaFromStorageNode(storageNodeSocket);
        logger.info("Controller: Get Meta From StorageNode");
        storageNodeSocket.close();
    }

    public static void getFullMetaFromStorageNode(Socket storageNodeSocket) throws IOException, InterruptedException {
        long currentTime = System.currentTimeMillis();
        long end = currentTime + REPLY_WAITING_TIME;
        ControllerMessages.ControllerMessageWrapper msgWrapper =
                ControllerMessages.ControllerMessageWrapper.parseDelimitedFrom(storageNodeSocket.getInputStream());

        logger.info("Controller: Waiting For Reply Of Getting Meta From StorageNode");
        while (System.currentTimeMillis() < end) {
            if (msgWrapper.hasMetaDataMsg()) {

                ControllerMessages.RetrieveMetaData metaDataMsg =
                        msgWrapper.getMetaDataMsg();
                String storageNodeHostName = storageNodeSocket.getInetAddress().getCanonicalHostName();
                int metaCount = metaDataMsg.getMetaCount();
                for (int i = 0; i < metaCount; i++) {
                    String fileName = metaDataMsg.getMeta(i).getFilename();
                    int chunkId = metaDataMsg.getMeta(i).getChunkId();

                    logger.info("Controller: Receive MetaData FileName: " + fileName + " ChunkId: " +chunkId
                                +" From " + storageNodeHostName);
                    storeFileInfo(storageNodeHostName, fileName, chunkId);
                }
                return;
            }
            Thread.sleep(500);
        }
        if (System.currentTimeMillis() < end) {
            logger.info("Controller: Cannot Get Meta From " + storageNodeSocket
                    .getInetAddress().getCanonicalHostName() + "!");
        }
//        controllerSocket.close();
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

//        if (!hostMetaMap.keySet().contains(storageHostName)) {
//            List<String> fileAndChunk = new ArrayList<String>();
//            fileAndChunk.add(fileName + "_" + chunkId);
//            hostMetaMap.put(storageHostName, fileAndChunk);
//        } else {
            hostMetaMap.get(storageHostName).add(fileName + "_" + chunkId);
//        }
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

    public static void detectFailureNode() throws ParseException, IOException {
        Date currentDate = new Date();

        for (int i = 0; i < storageNodeList.size(); i++) {
            STNode stNode = storageNodeList.get(i);
            String storageNodeHostName = stNode.storageNodeHostName;
            Date storageNodeDate = dateFormat.parse(heartBeatMap.get(storageNodeHostName));
            long dateDiff = (currentDate.getTime() - storageNodeDate.getTime()) / MILLIS_PER_SEC;
            if (dateDiff >= FAILURE_NODE_TIME) {
                logger.info("Controller: StorageNode " + storageNodeHostName + " Crashed Down!");
                storageNodeList.remove(stNode);
                heartBeatMap.remove(storageNodeHostName);
                repairStorageNode(storageNodeHostName);
                i--;
            }
        }
    }

    public static void repairStorageNode(String storageNodeHostName) throws IOException {
        List<String> fileAndChunk = hostMetaMap.get(storageNodeHostName);
        hostMetaMap.remove(storageNodeHostName);
        for (String filechunk : fileAndChunk) {
            List<String> aNodes = new ArrayList<String>();
            List<String> bNodes = new ArrayList<String>();
            for (String storageNodeName : hostMetaMap.keySet()) {
                logger.info("Controller: Still Alive StorageNode: " + storageNodeName);
                if (hostMetaMap.get(storageNodeName).contains(filechunk)) {
                    aNodes.add(storageNodeName);
                } else {
                    bNodes.add(storageNodeName);
                }
            }
            logger.info("Controller: aNode size " + aNodes.size() + " bNode size " + bNodes.size() + " host: " + hostMetaMap.size());
            int i = 0, j = 0;
            while (i < aNodes.size() && j < bNodes.size()) {
                Socket storageNodeSocket = null;
                try {
                    logger.info("Controller: Start Send Message To StorageNode " + aNodes.get(i)
                            + " To Repair StorageNode " + storageNodeHostName);

                    storageNodeSocket = new Socket(aNodes.get(i), STORAGENODE_PORT);
                    logger.info("Controller: Get socket");

                    StorageMessages.RepairNode.Builder repairNodeMsg =
                            StorageMessages.RepairNode.newBuilder();

                    logger.info("Controller: Get Builder");

                    repairNodeMsg.setFileName(filechunk.split("_")[0])
                            .setChunkId(Integer.parseInt(filechunk.split("_")[1]))
                            .setHostName(bNodes.get(j))
                            .build();
                    logger.info("Controller: Set Message FileName: " + filechunk.split("_")[0]
                            + " ChunkId: " + Integer.parseInt(filechunk.split("_")[1])
                            + " HostName: " + bNodes.get(j));

                    StorageMessages.StorageMessageWrapper msgWrapper =
                            StorageMessages.StorageMessageWrapper.newBuilder()
                                    .setRepairNodeMsg(repairNodeMsg)
                                    .build();
                    logger.info("Controller: Set RepairMessage");

                    msgWrapper.writeDelimitedTo(storageNodeSocket.getOutputStream());
                    logger.info("Controller: Finished Send Message To StorageNode " + aNodes.get(i)
                            + " To Repair StorageNode " + storageNodeHostName);
                } catch(IOException e) {
                    logger.info("Controller: StorageNode " + aNodes.get(i) + " Also Crashed Down");
                    i++;
                }
                if (!checkIfRepaired(storageNodeSocket)) {
                    j++;
                } else {
                    break;
                }
//                storageNodeSocket.close();
            }
        }
    }

    public static boolean checkIfRepaired(Socket socket) throws IOException{
        logger.info("Controller: Check If It Repaired");
        ControllerMessages.ControllerMessageWrapper msgWrapper =
                ControllerMessages.ControllerMessageWrapper.parseDelimitedFrom(
                        socket.getInputStream());
        if (msgWrapper.hasRepairNodeMsg()) {
            ControllerMessages.SendRepairNode repairNodeMsg =
                    msgWrapper.getRepairNodeMsg();
            socket.close();
            logger.info("Controller: Get If It Repaired Reply: " + repairNodeMsg.getIsSend());
            return repairNodeMsg.getIsSend();
        }
        socket.close();
        return false;
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
