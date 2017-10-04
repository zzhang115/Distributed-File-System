package edu.usfca.cs.dfs;

import com.google.protobuf.ByteString;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
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

public class StorageNode {
    private static ServerSocket nodeServerSocket;
    private static Socket nodeSocket;
    private static DateFormat dateFormat;
    private static Map<String, List<Integer>> updateMetaMap;
    private static Map<String, List<Integer>> fullMetaMap;
    private static ReentrantLock lock = new ReentrantLock();

    public static void main(String[] args)
    throws Exception {
        String hostname = getHostname();
        System.out.println("Starting storage node on " + hostname + "...");
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
                System.out.println("Send HeartBeat! --" + dateFormat.format(new Date()));
                try {
                    lock.lock();
                    sendHeartBeat();
                    lock.unlock();
                    System.out.println("unlock from heart");
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        };

        ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
        executor.scheduleAtFixedRate(heartBeat, 0, 5, TimeUnit.SECONDS);
    }

    public static void mainFunction() throws IOException {
        while (true) {
            System.out.println("Listening...");
            Socket socket = nodeServerSocket.accept();
            StorageMessages.StorageMessageWrapper msgWrapper
                = StorageMessages.StorageMessageWrapper.parseDelimitedFrom(
                        socket.getInputStream());

            if (msgWrapper.hasStoreChunkMsg()) {
                StorageMessages.StoreChunk storeChunkMsg
                    = msgWrapper.getStoreChunkMsg();
                String fileName = storeChunkMsg.getFileName();
                int chunkId = storeChunkMsg.getChunkId();
                System.out.println("Storing file name: " + fileName);
                System.out.println("Storing file ID: " + chunkId);

                lock.lock();
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
                lock.unlock();

                ByteString data = storeChunkMsg.getData();
                String dataStr = data.toStringUtf8();
                System.out.println("Storing file data: " + dataStr);

                File chunk = new File("storage.file/" + fileName + "_Chunk" + chunkId);
                BufferedWriter writer = new BufferedWriter(new FileWriter(chunk));
                writer.write(dataStr);
                writer.flush();
                writer.close();
            }
        }
    }

    public static void sendHeartBeat() throws IOException {
        nodeSocket = new Socket("localhost", 8080);
        String curPath = System.getProperty("user.dir");
        int index = 0;

        ControllerMessages.HeartBeatSignal.Builder heartBeatMsg =
                        ControllerMessages.HeartBeatSignal.newBuilder();

        for (Map.Entry<String, List<Integer>> meta : updateMetaMap.entrySet()) {
            for (int chunkId : meta.getValue()) {
                heartBeatMsg.addMetaBuilder().setFilename(meta.getKey()).setChunkId(chunkId);
            }
        }

        double usableSpace = (double) new File(curPath).getUsableSpace();   // more precisely than getFreeSpace()
        heartBeatMsg.setFreeSpace(usableSpace)
                .setTimestamp(dateFormat.format(new Date()))
                .build();
//                ControllerMessages.HeartBeatSignal heartBeatMsg =
//                        ControllerMessages.HeartBeatSignal.newBuilder()
//                                .addMetaBuilder().setFilename(meta.getKey()).setChunkId(chunkId)
//                                .setFreeSpace(usableSpace)
//                                .setTimestamp(dateFormat.format(new Date()))
//                                .build();
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
