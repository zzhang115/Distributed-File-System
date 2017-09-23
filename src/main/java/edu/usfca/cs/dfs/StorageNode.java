package edu.usfca.cs.dfs;

import com.google.protobuf.ByteString;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class StorageNode {
    private static ServerSocket nodeServerSocket;
    private static Socket nodeSocket;
    private static DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");

    public static void main(String[] args)
    throws Exception {
        String hostname = getHostname();
        System.out.println("Starting storage node on " + hostname + "...");
        storageNodeInit();
        mainFunction();
    }

    public static void storageNodeInit() throws IOException {
        nodeServerSocket = new ServerSocket(9090);
        nodeSocket = new Socket("localhost", 8080);

        Runnable heartBeat = new Runnable() {
            public void run() {
                System.out.println("Send HeartBeat! --" + dateFormat.format(new Date()));
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
        StorageMessages.HeartBeatSignal heartBeatMsg
                = StorageMessages.HeartBeatSignal.newBuilder()
                .setStorageNodeID()
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
