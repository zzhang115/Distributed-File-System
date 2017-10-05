package edu.usfca.cs.dfs;


import com.google.protobuf.ByteString;

import java.io.*;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.logging.Logger;

public class Client {
    private static Logger logger = Logger.getLogger("Log");
    private static String filePath = "client.file/test.pdf";
//    private static String filePath = "client.file/data_co.csv";
    private static Socket controllerSocket;
    private static Socket storageNodeSocket;
    private static List<DFSChunk> chunks= new ArrayList<DFSChunk>();
    private static List<String> availStorageNodeHostName = new ArrayList<String>();
    private static long fileSize;
    private static final int SIZE_OF_CHUNK = 20;//1024 * 1024; // 1MB
    private static final int REPLY_WAITING_TIME = 10000;

    public static void main(String[] args) throws IOException, InterruptedException {
        logger.info("Client: Start break file to chunks.");
        clientInit();
        logger.info("Client: Finish breaking chunks.");
    }

    public static void clientInit() throws IOException, InterruptedException {
        controllerSocket = new Socket("localhost", 8080);
        storageNodeSocket = new Socket("localhost", 9090);
        File file = new File(filePath);
        fileSize = file.length();
        System.out.println("fileSize:" + fileSize);
//        String md5Hash = fileCheckSum(file);
        breakFiletoChunks(file);
        sendRequestToController(fileSize);
        getReplyFromController();
//        for (DFSChunk chunk : chunks) {
//            sendStoreRequestToStorageNode(chunk);
//        }
        controllerSocket.close();
        storageNodeSocket.close();
    }

    public static void sendRequestToController(long fileSize) throws IOException {

        String request = "StoreFile:" + fileSize;
        ControllerMessages.StoreChunkRequest storeChunkRequestMsg
                = ControllerMessages.StoreChunkRequest.newBuilder()
                .setFileSize(fileSize)
                .build();
        ControllerMessages.ControllerMessageWrapper msgWrapper =
                ControllerMessages.ControllerMessageWrapper.newBuilder()
                        .setStoreChunkRequestMsg(storeChunkRequestMsg)
                        .build();
        msgWrapper.writeDelimitedTo(controllerSocket.getOutputStream());
    }

    public static void getReplyFromController() throws IOException, InterruptedException {
        long currentTime = System.currentTimeMillis();
        long end = currentTime + REPLY_WAITING_TIME;
        ClientMessages.ClientMessageWrapper msgWrapper
                = ClientMessages.ClientMessageWrapper.parseDelimitedFrom(
                        controllerSocket.getInputStream());
        while (System.currentTimeMillis() < end) {
            if (msgWrapper.hasAvailstorageNodeMsg()) {
                ClientMessages.AvailStorageNode availStorageNodeMsg
                        = msgWrapper.getAvailstorageNodeMsg();
                for (int i = 0; i < availStorageNodeMsg.getStorageNodeHostNameCount(); i++) {
                    availStorageNodeHostName.add(availStorageNodeMsg.getStorageNodeHostName(i));
                }
                return;
            }
            Thread.sleep(500);
        }
        if (System.currentTimeMillis() < end) {
            System.out.println("Controller is out of service now!");
        }
    }

    public static void sendStoreRequestToStorageNode(DFSChunk chunk) throws IOException {

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

        msgWrapper.writeDelimitedTo(storageNodeSocket.getOutputStream());
    }

    public static void breakFiletoChunks(File file) throws IOException {
        byte[] buffer = new byte[SIZE_OF_CHUNK];
        String fileName = file.getName();
        try (FileInputStream fileInputStream = new FileInputStream(file);
             BufferedInputStream bufferedInputStream = new BufferedInputStream(fileInputStream)) {
            int chunksId = 0;
            int length;
            while ((length = bufferedInputStream.read(buffer)) > 0) {
                byte[] newBuffer = new byte[length];
                System.arraycopy(buffer, 0, newBuffer, 0, length);
                ByteString data = ByteString.copyFrom(buffer);
                DFSChunk dfsChunk = new DFSChunk(fileName, chunksId++, data);
                chunks.add(new DFSChunk(fileName, chunksId, data));
//                System.out.println(dfsChunk.getData().toStringUtf8());
            }
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

        FileInputStream fileInputStream = new FileInputStream(file);
        byte[] byteArray = new byte[1024]; //Create byte array to read data in chunks
        int bytesCount = 0;

        //Read file data and update in message digest
        while ((bytesCount = fileInputStream.read(byteArray)) != -1) {
            md5Digest.update(byteArray, 0, bytesCount);
        };
        fileInputStream.close();

        //Get the hash's bytes
        //This bytes[] has bytes in decimal format;
        byte[] bytes = md5Digest.digest();

        //Convert it to hexadecimal format
        StringBuilder sb = new StringBuilder();
        for(int i=0; i< bytes.length ;i++) {
            sb.append(Integer.toString((bytes[i] & 0xff) + 0x100, 16).substring(1));
        }
        return sb.toString();
    }

}
