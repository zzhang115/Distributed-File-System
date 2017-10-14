package edu.usfca.cs.dfs;

import com.google.protobuf.ByteString;

import java.io.*;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.file.Path;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Logger;

public class StorageNode {
    private static Logger logger = Logger.getLogger("Log");
    private static ServerSocket nodeServerSocket;
    private static Socket nodeSocket;
    private static DateFormat dateFormat;
    private static volatile Map<String, List<Integer>> updateMetaMap; // <fileName, <chunkId>>
    private static Map<String, List<Integer>> fullMetaMap; // <fileName, <chunkId>>
    private static ReentrantLock lock = new ReentrantLock();
    private static final String CONTROLLER_HOSTNAME = "bass01.cs.usfca.edu";
    private static String storeFilePath = "/home2/zzhang115/";
    private static final int CONTROLLER_PORT = 40000;
    private static final int STORAGENODE_PORT= 40010;

    public static void main(String[] args) throws Exception {
        String hostname = getHostname();
        storageNodeInit();
        while (true) {
            handleMessage();
        }
    }

    public static void storageNodeInit() throws IOException {
        System.setProperty("java.util.logging.SimpleFormatter.format",
                "%5$s%6$s -- %1$tF %1$tT %4$s %2$s%n");
        logger.info("StorageNode: Initializing...");

        nodeServerSocket = new ServerSocket(STORAGENODE_PORT);
        dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        updateMetaMap = new HashMap<String, List<Integer>>();
        fullMetaMap = new HashMap<String, List<Integer>>();
        clearStoreFilePath();

        Runnable heartBeat = new Runnable() {
            public void run() {
//                logger.info("StorageNode: Send HeartBeat! --" + dateFormat.format(new Date()));
                try {
                    lock.lock();
                    sendHeartBeat();
                    lock.unlock();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        };

        ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
        executor.scheduleAtFixedRate(heartBeat, 0, 5, TimeUnit.SECONDS);
    }

    public static void clearStoreFilePath() throws IOException {
        File dir = new File(storeFilePath);
        for (File file : dir.listFiles()) {
            if (!file.isDirectory()) {
                file.delete();
            }
        }
    }

    public static void handleMessage() throws IOException {
        Socket socket = nodeServerSocket.accept();
        StorageMessages.StorageMessageWrapper msgWrapper
                = StorageMessages.StorageMessageWrapper.parseDelimitedFrom(
                socket.getInputStream());

        if (msgWrapper.hasStoreChunkMsg()) {
            logger.info("Storage: Receive Store Chunk Request From " + socket.getInetAddress());
            StorageMessages.StoreChunk storeChunkMsg
                    = msgWrapper.getStoreChunkMsg();

            // storing chunk info into map in order to update info to controller
            // still need to solve a problem that is that if chunk stored failed, don't update info to controller
            int storageHostNameCount = storeChunkMsg.getHostNameCount();
            List<String> copyChunkStorageNodeHostNames = new ArrayList<String>();
            for (int i = 0; i < storageHostNameCount; i++) {
                copyChunkStorageNodeHostNames.add(storeChunkMsg.getHostName(i));
            }

            String fileName = storeChunkMsg.getFileName();
            int chunkId = storeChunkMsg.getChunkId();
            int copies = storeChunkMsg.getCopies();
            ByteString data = storeChunkMsg.getData();

            logger.info("StorageNode: Storing File Name: " + fileName + " ChunkId: " + chunkId);

            if (fullMetaMap.keySet().contains(fileName)) {
                fullMetaMap.get(fileName).add(chunkId);
            } else {
                List<Integer> chunkIdList = new ArrayList<Integer>();
                chunkIdList.add(chunkId);
                fullMetaMap.put(fileName, chunkIdList);
            }

            if (updateMetaMap.keySet().contains(fileName)) {
                updateMetaMap.get(fileName).add(chunkId);
            } else {
                List<Integer> chunkIdList = new ArrayList<Integer>();
                chunkIdList.add(chunkId);
                updateMetaMap.put(fileName, chunkIdList);
            }

            writeFileToLocalMachine(fileName, chunkId, data);
            logger.info("StorageNode: " + getHostname() + " Store Chunk Successfully!");
            socket.close();
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            passChunkToPeer(copyChunkStorageNodeHostNames, fileName, chunkId, copies - 1, data);
            return;
        }

        if (msgWrapper.hasRetrieveFileMsg()) {
            StorageMessages.RetrieveFile retrieveFileMsg =
                    msgWrapper.getRetrieveFileMsg();
            logger.info("StorageNode: Received Retrieve File Request!");
            String fileName = retrieveFileMsg.getFileName();
            int chunkId = retrieveFileMsg.getChunkId();
            logger.info("StorageNode: Retrieve Request, FileName: " + fileName + " ChunkId: " + chunkId);
            if (fullMetaMap.containsKey(fileName) && fullMetaMap.get(fileName).contains(chunkId)) {
                sendFileDataToClient(socket, fileName, chunkId);
            }
            socket.close();
            return;
        }
    }

    public static void passChunkToPeer(List<String> copyChunkStorageNodeHostNames, String fileName,
                                       int chunkId, int copies, ByteString data) throws IOException {
        if (copies > 0) {
            String hostName = copyChunkStorageNodeHostNames.get(0);
            Socket storageNodeSocket = new Socket(hostName, STORAGENODE_PORT);
            logger.info("StorageNode: Send Store Request To Peer StorageNode: " + hostName
                    + " To Store Chunk" + chunkId);

            StorageMessages.StoreChunk.Builder storeChunkMsg =
                    StorageMessages.StoreChunk.newBuilder();
            // form send pipeline
            for (int i = 1; i < copyChunkStorageNodeHostNames.size(); i++) {
                storeChunkMsg.addHostName(copyChunkStorageNodeHostNames.get(i));
            }

            storeChunkMsg.setFileName(fileName).setChunkId(chunkId)
                    .setData(data)
                    .setCopies(copies)
                    .build();

            StorageMessages.StorageMessageWrapper msgWrapper =
                    StorageMessages.StorageMessageWrapper.newBuilder()
                            .setStoreChunkMsg(storeChunkMsg)
                            .build();
            msgWrapper.writeDelimitedTo(storageNodeSocket.getOutputStream());
            storageNodeSocket.close();
            logger.info("StorageNode: Finishing Send Store Request To Peer StorageNode: " + hostName
                    + " To Store Chunk" + chunkId);
        }
    }

    public static void sendFileDataToClient(Socket socket, String fileName, int chunkId) throws IOException {
        logger.info("StorageNode: Start Sending File Data To Client");
        File file = new File(storeFilePath + fileName + "_Chunk" + chunkId);
        byte[] dataBytes = new byte[(int)file.length()];
        FileInputStream fileInputStream = new FileInputStream(file);
        fileInputStream.read(dataBytes);
        fileInputStream.close();
        ByteString data = ByteString.copyFrom(dataBytes);

        ClientMessages.RetrieveFileData.Builder retrieveFileDataMsg =
                ClientMessages.RetrieveFileData.newBuilder();
        retrieveFileDataMsg.setFileName(fileName)
                .setChunkID(chunkId)
                .setData(data)
                .build();
        ClientMessages.ClientMessageWrapper msgWrapper =
                ClientMessages.ClientMessageWrapper.newBuilder()
                        .setRetrieveFileDataMsg(retrieveFileDataMsg)
                        .build();
        msgWrapper.writeDelimitedTo(socket.getOutputStream());
        logger.info("StorageNode: Finished Sending File Data To Client");
    }

    public static void writeFileToLocalMachine
            (String fileName, int chunkId, ByteString data) throws IOException {

        FileOutputStream fileOutputStream = new FileOutputStream(storeFilePath + fileName + "_Chunk" + chunkId);
        byte[] dataBytes = data.toByteArray();

        fileOutputStream.write(dataBytes);
        fileOutputStream.flush();
        fileOutputStream.close();
    }

    public static void sendHeartBeat() throws IOException {
        nodeSocket = new Socket(CONTROLLER_HOSTNAME, CONTROLLER_PORT);
        String curPath = System.getProperty("user.dir");

        ControllerMessages.HeartBeatSignal.Builder heartBeatMsg =
                        ControllerMessages.HeartBeatSignal.newBuilder();

        for (Map.Entry<String, List<Integer>> meta : updateMetaMap.entrySet()) {
            for (int chunkId : meta.getValue()) {
                heartBeatMsg.addMetaBuilder().setFilename(meta.getKey()).setChunkId(chunkId);
            }
        }

        double usableSpace = (double) new File(curPath).getUsableSpace();   // more precisely than getFreeSpace()
        heartBeatMsg.setFreeSpace(usableSpace).setTimestamp(
                dateFormat.format(new Date())).build();
        ControllerMessages.ControllerMessageWrapper msgWrapper =
                ControllerMessages.ControllerMessageWrapper.newBuilder()
                        .setHeartBeatSignalMsg(heartBeatMsg)
                        .build();
        msgWrapper.writeDelimitedTo(nodeSocket.getOutputStream());

        nodeSocket.close();
        updateMetaMap.clear();
    }

    /**
     * Retrieves the short host name of the current host.
     *
     * @return name of the current host
     */
    private static String getHostname() throws UnknownHostException {
        return InetAddress.getLocalHost().getHostName();
    }
}
