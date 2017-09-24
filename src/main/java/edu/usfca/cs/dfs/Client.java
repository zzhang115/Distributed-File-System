package edu.usfca.cs.dfs;


import com.google.protobuf.ByteString;

import java.io.*;
import java.net.Socket;
import java.util.*;
import java.util.logging.Logger;

public class Client {
    private static Logger logger = Logger.getLogger("Log");
    private static String filePath = "client.file/pig.txt";
    private static long fileSize;
    private static Socket socket;
    private static final int SIZEOFCHUNK = 20;
    private static List<DFSChunk> chunks= new ArrayList<DFSChunk>();

    public static void main(String[] args) throws IOException {
        logger.info("Client: Start break file to chunks.");
        clientInit();
        logger.info("Client: Finish breaking chunks.");
    }

    public static void clientInit() throws IOException {
        File file = new File(filePath);
        fileSize = file.length();
        breakFiletoChunks(file);
//        sendRequestToController(fileSize);
        for (DFSChunk chunk : chunks) {
            sendRequestToStorageNode(chunk);
        }
    }

    public static void sendRequestToController(long fileSize) throws IOException {
        socket = new Socket("localhost", 8080);

        String request = "StoreFile:" + fileSize;
        StorageMessages.StoreChunkRequest storeChunkRequestMsg
                = StorageMessages.StoreChunkRequest.newBuilder()
                .setFileSize(fileSize)
                .build();
        StorageMessages.StorageMessageWrapper msgWrapper =
                StorageMessages.StorageMessageWrapper.newBuilder()
                        .setStoreChunkRequestMsg(storeChunkRequestMsg)
                        .build();
        msgWrapper.writeDelimitedTo(socket.getOutputStream());

        socket.close();
    }

    public static void sendRequestToStorageNode(DFSChunk chunk) throws IOException {
        socket = new Socket("localhost", 9090);

        StorageMessages.StoreChunk storeChunkMsg
                = StorageMessages.StoreChunk.newBuilder()
                .setFileName(chunk.getChunkName())
                .setChunkId(chunk.getChunkID())
                .setData(chunk.getData())
                .build();

        StorageMessages.StorageMessageWrapper msgWrapper =
                StorageMessages.StorageMessageWrapper.newBuilder()
                        .setStoreChunkMsg(storeChunkMsg)
                        .build();

        msgWrapper.writeDelimitedTo(socket.getOutputStream());

        socket.close();
    }

    public static void breakFiletoChunks(File file) throws IOException {
        int sizeOfChunk = SIZEOFCHUNK;
        byte[] buffer = new byte[sizeOfChunk];
        String fileName = file.getName();
        try (FileInputStream fileInputStream = new FileInputStream(file);
             BufferedInputStream bufferedInputStream = new BufferedInputStream(fileInputStream)) {
            int chunksId = 0;
            int length;
            while ((length = bufferedInputStream.read(buffer)) > 0) {
                ByteString data = ByteString.copyFromUtf8(new String(buffer).substring(0, length));
                DFSChunk dfsChunk = new DFSChunk(fileName, chunksId++, data);
                chunks.add(new DFSChunk(fileName, chunksId, data));
                System.out.println(dfsChunk.getData().toStringUtf8());
            }
//            DataOutputStream
        }
    }

}
