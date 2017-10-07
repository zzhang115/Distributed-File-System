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
    private static String filePath = "client.file/test.pdf";
    private static String retrieveFilePath = "client.retrieve.file";
    private static String testRetrieveFileName = "test.pdf";
//    private static String filePath = "client.file/data_co.csv";
    private static Socket controllerSocket;
    private static Socket storageNodeSocket;
    private static List<DFSChunk> chunks= new ArrayList<DFSChunk>();
    private static List<String> availStorageNodeHostNames = new ArrayList<String>();
    private static long fileSize;
    private static final int SIZE_OF_CHUNK = 1024 * 1024; // 1MB
    private static final String CONTROLLER_HOSTNAME = "localhost";
    private static final int REPLY_WAITING_TIME = 10000;
    private static final int RETRIEVE_WAITING_TIME = 5000;
    private static final int CONTROLLER_PORT = 8080;
    private static final int STORAGENODE_PORT = 9090;

    public static void main(String[] args) throws IOException, InterruptedException {
        clientStoreFile();
        // at least wait 5 secs(heart beat interval) to keep file info has been registered in metadata
        Thread.sleep(RETRIEVE_WAITING_TIME);
        clientRetrieveFile();
    }

    public static void clientStoreFile() throws IOException, InterruptedException {
        logger.info("Client: Start Send Storing File Request");
        File file = new File(filePath);
        fileSize = file.length();
        System.out.println("fileSize:" + fileSize);
        String md5Hash = fileCheckSum(file);
        logger.info("Client: Start break file to chunks.");
        breakFiletoChunks(file);
        logger.info("Client: Finish breaking chunks.");

        sendStoreRequestToController(fileSize);
        getStoringReplyFromController();
        sendStoreRequestToStorageNode();
    }

    public static void clientRetrieveFile() throws IOException, InterruptedException {
        logger.info("Client: Send Retrieving File Request: " + testRetrieveFileName);
        sendRetrieveFileRequestToController(testRetrieveFileName);
        logger.info("Client: Wait Msg For Retrievng File: " + testRetrieveFileName);
        getRetrievingReplyFromController();
    }

    public static void sendStoreRequestToController(long fileSize) throws IOException {
        controllerSocket = new Socket(CONTROLLER_HOSTNAME, CONTROLLER_PORT);
        ControllerMessages.StoreChunkRequest storeChunkRequestMsg = ControllerMessages
                .StoreChunkRequest.newBuilder().setFileSize(fileSize).build();

        ControllerMessages.ControllerMessageWrapper msgWrapper = ControllerMessages
                .ControllerMessageWrapper.newBuilder()
                .setStoreChunkRequestMsg(storeChunkRequestMsg)
                .build();
        msgWrapper.writeDelimitedTo(controllerSocket.getOutputStream());
    }

    public static void getStoringReplyFromController() throws IOException, InterruptedException {
        long currentTime = System.currentTimeMillis();
        long end = currentTime + REPLY_WAITING_TIME;
        ClientMessages.ClientMessageWrapper msgWrapper
                = ClientMessages.ClientMessageWrapper.parseDelimitedFrom(
                        controllerSocket.getInputStream());
        while (System.currentTimeMillis() < end) {
            if (msgWrapper.hasAvailstorageNodeMsg()) {
                ClientMessages.AvailStorageNode availStorageNodeMsg
                        = msgWrapper.getAvailstorageNodeMsg();
                int count = availStorageNodeMsg.getStorageNodeHostNameCount();
                for (int i = 0; i < count; i++) {
                    logger.info("Receive StorageNode HostName: " +
                            availStorageNodeMsg.getStorageNodeHostName(i));
                    availStorageNodeHostNames.add(availStorageNodeMsg.getStorageNodeHostName(i));
                }
                return;
            }
            Thread.sleep(500);
        }
        if (System.currentTimeMillis() < end) {
            System.out.println("Controller is out of service now!");
        }
        controllerSocket.close();
    }

    public static void sendRetrieveFileRequestToController(String fileName) throws IOException {
        controllerSocket = new Socket(CONTROLLER_HOSTNAME, CONTROLLER_PORT);
        ControllerMessages.RetrieveFileRequest retrieveFileMsg = ControllerMessages
                .RetrieveFileRequest.newBuilder().setFilename(fileName).build();
        ControllerMessages.ControllerMessageWrapper msgWrapper = ControllerMessages
                .ControllerMessageWrapper.newBuilder()
                .setRetrieveFileMsg(retrieveFileMsg)
                .build();
        msgWrapper.writeDelimitedTo(controllerSocket.getOutputStream());
    }

    public static void getRetrievingReplyFromController() throws IOException, InterruptedException {
        long currentTime = System.currentTimeMillis();
        long end = currentTime + REPLY_WAITING_TIME;
        ClientMessages.ClientMessageWrapper msgWrapper
                = ClientMessages.ClientMessageWrapper.parseDelimitedFrom(
                        controllerSocket.getInputStream()); // wait here until there is a message

        while (System.currentTimeMillis() < end) {
            if (msgWrapper.hasReplyForRetrievingMsg()) {
                ClientMessages.ReplyForRetrieving retrievingFileMsg
                        = msgWrapper.getReplyForRetrievingMsg();
                int count = retrievingFileMsg.getRetrieveFileInfoCount();
                logger.info("chunk num:" + count);
                return;
            }
            Thread.sleep(500);
        }
        controllerSocket.close();
    }

    public static void sendStoreRequestToStorageNode() throws IOException {
        if (availStorageNodeHostNames.size() > 0) {
            String hostName = availStorageNodeHostNames.get(0);
            for (DFSChunk chunk : chunks) {
                storageNodeSocket = new Socket(hostName, STORAGENODE_PORT);
                System.out.println("chunkID " + chunk.getChunkID());

                StorageMessages.StoreChunk.Builder storeChunkMsg =
                        StorageMessages.StoreChunk.newBuilder();

                for (int i = 1; i < availStorageNodeHostNames.size(); i++) {
                    storeChunkMsg.addHostName(availStorageNodeHostNames.get(i));
                }

                storeChunkMsg.setFileName(chunk.getChunkName()).setChunkId(chunk.getChunkID())
                        .setData(chunk.getData())
                        .build();

                StorageMessages.StorageMessageWrapper msgWrapper =
                        StorageMessages.StorageMessageWrapper.newBuilder()
                                .setStoreChunkMsg(storeChunkMsg)
                                .build();

                msgWrapper.writeDelimitedTo(storageNodeSocket.getOutputStream());
                storageNodeSocket.close();
            }
        }
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
                ByteString data = ByteString.copyFrom(newBuffer);
                chunks.add(new DFSChunk(fileName, chunksId++, data));
            }
            System.out.println("chunks size: " + chunks.size());
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
