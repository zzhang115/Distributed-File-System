package edu.usfca.cs.dfs;


import com.google.protobuf.ByteString;

import java.io.*;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.logging.Logger;

public class Client {
    private static Logger logger = Logger.getLogger("Log");
    private static String filePath = "client.file/pig.txt";
    private static Socket socket;
    private static List<DFSChunk> chunks= new ArrayList<DFSChunk>();
    private static long fileSize;
    private static final int SIZE_OF_CHUNK = 20;

    public static void main(String[] args) throws IOException {
        logger.info("Client: Start break file to chunks.");
        clientInit();
        logger.info("Client: Finish breaking chunks.");
    }

    public static void clientInit() throws IOException {
        File file = new File(filePath);
        fileSize = file.length();
        String md5Hash = fileCheckSum(file);
        breakFiletoChunks(file);
//        sendRequestToController(fileSize);
        for (DFSChunk chunk : chunks) {
            sendRequestToStorageNode(chunk);
        }
    }

    public static void sendRequestToController(long fileSize) throws IOException {
        socket = new Socket("localhost", 8080);

        String request = "StoreFile:" + fileSize;
        ControllerMessages.StoreChunkRequest storeChunkRequestMsg
                = ControllerMessages.StoreChunkRequest.newBuilder()
                .setFileSize(fileSize)
                .build();
        ControllerMessages.ControllerMessageWrapper msgWrapper =
                ControllerMessages.ControllerMessageWrapper.newBuilder()
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
        int sizeOfChunk = SIZE_OF_CHUNK;
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

    public static String fileCheckSum(File file) throws IOException {
        //Use MD5 algorithm
        MessageDigest md5Digest = null;
        try {
            md5Digest = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

        FileInputStream fis = new FileInputStream(file);
        //Create byte array to read data in chunks
        byte[] byteArray = new byte[1024];
        int bytesCount = 0;

        //Read file data and update in message digest
        while ((bytesCount = fis.read(byteArray)) != -1) {
            md5Digest.update(byteArray, 0, bytesCount);
        };
        fis.close();

        //Get the hash's bytes
        //This bytes[] has bytes in decimal format;
        byte[] bytes = md5Digest.digest();

        //Convert it to hexadecimal format
        StringBuilder sb = new StringBuilder();
        for(int i=0; i< bytes.length ;i++) {
            sb.append(Integer.toString((bytes[i] & 0xff) + 0x100, 16).substring(1));
        }
        //complete hash
        return sb.toString();
    }

}
