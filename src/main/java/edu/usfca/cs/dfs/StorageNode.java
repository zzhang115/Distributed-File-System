package edu.usfca.cs.dfs;

import com.google.protobuf.ByteString;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;

public class StorageNode {
    private static ServerSocket serverSocket;
    public static void main(String[] args) 
    throws Exception {
        String hostname = getHostname();
        System.out.println("Starting storage node on " + hostname + "...");
        storageNodeInit();
    }

    public static void storageNodeInit() throws IOException {

    serverSocket = new ServerSocket(9090);
        System.out.println("Listening...");
        while (true) {
            Socket socket = serverSocket.accept();
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
