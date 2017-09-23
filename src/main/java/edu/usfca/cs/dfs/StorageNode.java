package edu.usfca.cs.dfs;

import com.google.protobuf.ByteString;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class StorageNode {
    private static ServerSocket nodeServerSocket;
    private static Socket nodeSocket;
    private static DateFormat dateFormat;
    private static Map<String, Integer> metamap;

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
        metamap = new HashMap<String, Integer>();

        Runnable heartBeat = new Runnable() {
            public void run() {
                System.out.println("Send HeartBeat! --" + dateFormat.format(new Date()));
                try {
                    sendHeartBeat();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        };

        ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
        executor.scheduleAtFixedRate(heartBeat, 0, 3, TimeUnit.SECONDS);
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
                System.out.println("Storing file name: "
                        + storeChunkMsg.getFileName());
                System.out.println("Storing file ID: " + storeChunkMsg.getChunkId());
                ByteString data = storeChunkMsg.getData();
                String dataStr = data.toStringUtf8();
                System.out.println("Storing file data: " + dataStr);
            }
        }
    }

    public static void sendHeartBeat() throws IOException {
        nodeSocket = new Socket("localhost", 8080);

        StringBuffer metaBuff = new StringBuffer();
        for (Map.Entry<String, Integer> meta : metamap.entrySet()) {
            metaBuff.append(meta.getKey() + ":" + meta.getValue() + ",");
        }
        if (metaBuff.length() > 0) {
            metaBuff.deleteCharAt(metaBuff.length() - 1);
        }

        String curPath = System.getProperty("user.dir");
        double usableSpace = (double) new File(curPath).getUsableSpace();   // more precisely than getFreeSpace()

        StorageMessages.HeartBeatSignal heartBeatMsg
                = StorageMessages.HeartBeatSignal.newBuilder()
                .setMetaData(metaBuff.toString())
                .setFreeSpace(usableSpace)
                .setTimestamp(dateFormat.format(new Date()))
                .build();
        StorageMessages.StorageMessageWrapper msgWrapper =
                StorageMessages.StorageMessageWrapper.newBuilder()
                        .setHeartBeatSignalMsg(heartBeatMsg)
                        .build();
        msgWrapper.writeDelimitedTo(nodeSocket.getOutputStream());
        nodeSocket.close();
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
