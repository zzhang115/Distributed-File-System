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
    private static Map<String, List<Integer>> updateMetaMap;
    private static Map<String, List<Integer>> fullMetaMap;
    private static ReentrantLock lock = new ReentrantLock();
    private static List<String> availStorageNodeHostNames = new ArrayList<String>();
    private static final int CONTROLLER_PORT = 8080;
    private static final int STORAGENODE_PORT= 9090;

    public static void main(String[] args)
    throws Exception {
        String hostname = getHostname();
        logger.info("StorageNode Initializing.");
        storageNodeInit();
        mainFunction();
    }

    public static void storageNodeInit() throws IOException {
        nodeServerSocket = new ServerSocket(9090);
        dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        updateMetaMap = new HashMap<String, List<Integer>>();
        fullMetaMap = new HashMap<String, List<Integer>>();

        Runnable heartBeat = new Runnable() {
            public void run() {
                logger.info("Send HeartBeat! --" + dateFormat.format(new Date()));
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

    public static void mainFunction() throws IOException {
        while (true) {
            handleStoreChunkRequest();
        }
    }

    public static void handleStoreChunkRequest() throws IOException {
        System.out.println("Listening...");
        Socket socket = nodeServerSocket.accept();
        StorageMessages.StorageMessageWrapper msgWrapper
                = StorageMessages.StorageMessageWrapper.parseDelimitedFrom(
                socket.getInputStream());

        if (msgWrapper.hasStoreChunkMsg()) {
            StorageMessages.StoreChunk storeChunkMsg
                    = msgWrapper.getStoreChunkMsg();

            int storageHostNameCount = storeChunkMsg.getHostNameCount();
            for (int i = 0; i < storageHostNameCount; i++) {
                availStorageNodeHostNames.add(storeChunkMsg.getHostName(i));
            }

            String fileName = storeChunkMsg.getFileName();
            int chunkId = storeChunkMsg.getChunkId();
            System.out.println("Storing file name: " + fileName);
            System.out.println("Storing file ID: " + chunkId);

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
    }

    public static void writeFileToLocalMachine
            (String fileName, int chunkId, ByteString data) throws IOException {
        FileOutputStream fileOutputStream = new FileOutputStream("storage.file/" + fileName + "_Chunk" + chunkId);
        System.out.println("old: "+data.size());
        byte[] dataBytes = data.toByteArray();
        System.out.println(dataBytes.length);
        ByteString bytes = ByteString.copyFrom(dataBytes);
        System.out.println("new: "+bytes.size());

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
