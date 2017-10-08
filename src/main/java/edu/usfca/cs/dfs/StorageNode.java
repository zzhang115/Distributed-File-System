package edu.usfca.cs.dfs;

import com.google.protobuf.ByteString;

import java.io.*;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
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
    private static Map<String, List<Integer>> updateMetaMap; // <fileName, <chunkId>>
    private static Map<String, List<Integer>> fullMetaMap; // <fileName, <chunkId>>
    private static ReentrantLock lock = new ReentrantLock();
    private static List<String> availStorageNodeHostNames = new ArrayList<String>();
    private static final int CONTROLLER_PORT = 8080;
    private static final int STORAGENODE_PORT= 9090;

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

        nodeServerSocket = new ServerSocket(9090);
        dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        updateMetaMap = new HashMap<String, List<Integer>>();
        fullMetaMap = new HashMap<String, List<Integer>>();

        Runnable heartBeat = new Runnable() {
            public void run() {
                logger.info("StorageNode: Send HeartBeat! --" + dateFormat.format(new Date()));
                try {
                    lock.lock();
                    sendHeartBeat();
                    lock.unlock();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        };

        ScheduledExecutorService executor = Executors.newScheduledThreadPool(1); executor.scheduleAtFixedRate(heartBeat, 0, 5, TimeUnit.SECONDS);
    }

    public static void handleMessage() throws IOException {
        Socket socket = nodeServerSocket.accept();
        StorageMessages.StorageMessageWrapper msgWrapper
                = StorageMessages.StorageMessageWrapper.parseDelimitedFrom(
                socket.getInputStream());

        if (msgWrapper.hasStoreChunkMsg()) {
            logger.info("Storage: Receive Store Chunk Request!");
            StorageMessages.StoreChunk storeChunkMsg
                    = msgWrapper.getStoreChunkMsg();

            // storing chunk info into map in order to update info to controller
            // still need to solve a problem that is that if chunk stored failed, don't update info to controller
            int storageHostNameCount = storeChunkMsg.getHostNameCount();
            for (int i = 0; i < storageHostNameCount; i++) {
                availStorageNodeHostNames.add(storeChunkMsg.getHostName(i));
            }

            String fileName = storeChunkMsg.getFileName();
            int chunkId = storeChunkMsg.getChunkId();
            logger.info("StorageNode: Storing file name: " + fileName);
            logger.info("StorageNode: Storing file ID: " + chunkId);

            if (fullMetaMap.keySet().contains(fileName)) {
                fullMetaMap.get(fileName).add(chunkId);
            } else {
                List<Integer> chunkIdList = new ArrayList<Integer>();
                chunkIdList.add(chunkId);
                fullMetaMap.put(fileName, chunkIdList);
            }

            lock.lock();
            if (updateMetaMap.keySet().contains(fileName)) {
                updateMetaMap.get(fileName).add(chunkId);
            } else {
                List<Integer> chunkIdList = new ArrayList<Integer>();
                chunkIdList.add(chunkId);
                updateMetaMap.put(fileName, chunkIdList);
            }
            lock.unlock();

            ByteString data = storeChunkMsg.getData();
            writeFileToLocalMachine(fileName, chunkId, data);
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
        }
        socket.close();
    }

    public static void sendFileDataToClient(Socket socket, String fileName, int chunkId) throws IOException {
        logger.info("StorageNode: Start Sending File Data To Client");
        File file = new File("storage.file/" + fileName + "_Chunk" + chunkId);
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

        FileOutputStream fileOutputStream = new FileOutputStream("storage.file/" + fileName + "_Chunk" + chunkId);
        byte[] dataBytes = data.toByteArray();
        ByteString bytes = ByteString.copyFrom(dataBytes);

        fileOutputStream.write(dataBytes);
        fileOutputStream.flush();
        fileOutputStream.close();
    }

    public static void sendHeartBeat() throws IOException {
        nodeSocket = new Socket("localhost", CONTROLLER_PORT);
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
    private static String getHostname()
    throws UnknownHostException {
        return InetAddress.getLocalHost().getHostName();
    }
}
