package edu.usfca.cs.dfs;


import com.google.protobuf.ByteString;

import java.io.*;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Logger;

public class Client {
    private static Logger logger = Logger.getLogger("Log");
    private static String filePath = "p1-zzhang115/client.file/test.pdf";
    private static String retrieveFilePath = "p1-zzhang115/client.retrieve.file/";
    private static String testRetrieveFileName = "test.pdf";
//    private static String filePath = "client.file/data_co.csv";
    private static Socket controllerSocket;
    private static Socket storageNodeSocket;
    private static List<DFSChunk> storeChunks = new ArrayList<DFSChunk>();
    private static List<DFSChunk> retrieveChunks = new ArrayList<DFSChunk>();
    private static List<String> availStorageNodeHostNames = new ArrayList<String>();
    private static Map<Integer, String> retrieveFileMap = new HashMap<Integer, String>();
    private static long fileSize;
    private static int retrieveChunkSum;
    private static final int SIZE_OF_CHUNK = 1024 * 1024; // 1MB
    private static final String CONTROLLER_HOSTNAME = "bass01.cs.usfca.edu";
    private static final int REPLY_WAITING_TIME = 10000;
    private static final int RETRIEVE_WAITING_TIME = 3000;
    private static final int CONTROLLER_PORT = 40000;
    private static final int STORAGENODE_PORT = 40010;
    private static CountDownLatch latch;

    public static void main(String[] args) throws IOException, InterruptedException {

        clientInit();
        clientStoreFile();
        // at least wait 5 secs(heart beat interval) to keep file info has been registered in metadata
        Thread.sleep(RETRIEVE_WAITING_TIME);
        clientRetrieveFile();
    }

    public static void clientInit() throws IOException {
        System.setProperty("java.util.logging.SimpleFormatter.format",
                "%5$s%6$s -- %1$tF %1$tT %4$s %2$s%n");
    }

    public static void clientStoreFile() throws IOException, InterruptedException {
//        System.setProperty("java.util.logging.SimpleFormatter.format",
//                "%1$tF %1$tT %4$s %2$s %5$s%6$s%n");
        logger.info("Client: Start Send Storing File Request");
        File file = new File(filePath);
        fileSize = file.length();
        logger.info("Client: FileSize is " + fileSize);
        String md5Hash = fileCheckSum(file);
        logger.info("Client: Start break file to chunks");
        breakFiletoChunks(file);
        logger.info("Client: Finish breaking chunks");

        sendStoreRequestToController(fileSize);
        getStoringReplyFromController();
        sendStoreRequestToStorageNode();
    }

    public static void clientRetrieveFile() throws IOException, InterruptedException {
        logger.info("Client: Send Retrieving File Request: " + testRetrieveFileName);
        sendRetrieveFileRequestToController(testRetrieveFileName);
        logger.info("Client: Wait Msg For Retrievng File: " + testRetrieveFileName);
        getRetrievingReplyFromController();
        sendRetrieveRequestToStorageNode(testRetrieveFileName);
        writeReceivedFileDataToLocal();
    }

    public static void sendStoreRequestToController(long fileSize) throws IOException {
        controllerSocket = new Socket(CONTROLLER_HOSTNAME, CONTROLLER_PORT);
        logger.info("Client: Start Sending Store Request To Controller");
        ControllerMessages.StoreChunkRequest storeChunkRequestMsg = ControllerMessages
                .StoreChunkRequest.newBuilder().setFileSize(fileSize).build();

        ControllerMessages.ControllerMessageWrapper msgWrapper = ControllerMessages
                .ControllerMessageWrapper.newBuilder()
                .setStoreChunkRequestMsg(storeChunkRequestMsg)
                .build();
        msgWrapper.writeDelimitedTo(controllerSocket.getOutputStream());
    }

    public static void getStoringReplyFromController() throws IOException, InterruptedException {
//        controllerSocket = new Socket(CONTROLLER_HOSTNAME, CONTROLLER_PORT);
//        new controllerSocket will lead program stuck at line 83 getInputStream();
//        in the same time, controller just receive this new controllerSocket, and stuck at socket.getInputStream()
//        so it cause storageNode cannot connect with controller, so it said carshed down

        long currentTime = System.currentTimeMillis();
        long end = currentTime + REPLY_WAITING_TIME;
        ClientMessages.ClientMessageWrapper msgWrapper
                = ClientMessages.ClientMessageWrapper.parseDelimitedFrom(
                        controllerSocket.getInputStream());
        logger.info("Client: Waiting For Reply Of Storing Request!");
        while (System.currentTimeMillis() < end) {
            if (msgWrapper.hasAvailstorageNodeMsg()) {
                ClientMessages.AvailStorageNode availStorageNodeMsg
                        = msgWrapper.getAvailstorageNodeMsg();
                retrieveChunkSum = availStorageNodeMsg.getStorageNodeHostNameCount();

                for (int i = 0; i < retrieveChunkSum; i++) {
                    logger.info("Client: Receive StorageNode HostName: " +
                            availStorageNodeMsg.getStorageNodeHostName(i));
                    availStorageNodeHostNames.add(availStorageNodeMsg.getStorageNodeHostName(i));
                }
                return;
            }
            Thread.sleep(500);
        }
        if (System.currentTimeMillis() < end) {
            logger.info("Client: Controller is out of service now!");
        }
        controllerSocket.close();
    }

    public static void sendStoreRequestToStorageNode() throws IOException {
        if (availStorageNodeHostNames.size() > 0) {
            for (int i = 0; i < availStorageNodeHostNames.size(); i++) {
                String hostName = availStorageNodeHostNames.get(i);
                for (DFSChunk chunk : storeChunks) {
                    storageNodeSocket = new Socket(hostName, STORAGENODE_PORT);
                    logger.info("Client: Send Store Request To StorageNode: " + hostName
                            + " To Store Chunk" + chunk.getChunkID());

                    StorageMessages.StoreChunk.Builder storeChunkMsg =
                            StorageMessages.StoreChunk.newBuilder();

                    // form send pipeline
                    for (int j = 1; j < availStorageNodeHostNames.size(); j++) {
                        storeChunkMsg.addHostName(availStorageNodeHostNames.get(j));
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
        } else {
            logger.info("Client: No StorageNode Is Available!");
        }
    }

    public static void sendRetrieveFileRequestToController(String fileName) throws IOException {
        controllerSocket = new Socket(CONTROLLER_HOSTNAME, CONTROLLER_PORT);
        logger.info("Client: Start Sending Retrieve File Request To Controller");
        ControllerMessages.RetrieveFileRequest retrieveFileMsg = ControllerMessages
                .RetrieveFileRequest.newBuilder()
                .setFilename(fileName)
                .build();

        ControllerMessages.ControllerMessageWrapper msgWrapper = ControllerMessages
                .ControllerMessageWrapper.newBuilder()
                .setRetrieveFileMsg(retrieveFileMsg)
                .build();
        msgWrapper.writeDelimitedTo(controllerSocket.getOutputStream());
        logger.info("Client: Finished Sending Retrieve File Request To Controller");
    }

    public static void getRetrievingReplyFromController() throws IOException, InterruptedException {
        long currentTime = System.currentTimeMillis();
        long end = currentTime + REPLY_WAITING_TIME;
        ClientMessages.ClientMessageWrapper msgWrapper
                = ClientMessages.ClientMessageWrapper.parseDelimitedFrom(
                        controllerSocket.getInputStream()); // wait here until there is a message

        logger.info("Client: Waiting For Reply Of Retrieving From Controller");
        while (System.currentTimeMillis() < end) {
            if (msgWrapper.hasReplyForRetrievingMsg()) {
                ClientMessages.ReplyForRetrieving retrievingFileMsg
                        = msgWrapper.getReplyForRetrievingMsg();
                int count = retrievingFileMsg.getRetrieveFileInfoCount();
                int chunkId;
                String storageNodeHostName;
                logger.info("Client: Chunk's Num: " + count);
                for (int i = 0; i < count; i++) {
                    chunkId = retrievingFileMsg.getRetrieveFileInfo(i).getChunkId();
                    storageNodeHostName = retrievingFileMsg.getRetrieveFileInfo(i).getStorageNodeHostName();
                    logger.info("Client: ChunkId: " + chunkId + " StorageNodeHostName: " + storageNodeHostName);
                    retrieveFileMap.put(chunkId, storageNodeHostName);
                }
                return;
            }
            Thread.sleep(500);
        }
        controllerSocket.close();
    }

    public static void sendRetrieveRequestToStorageNode(String fileName) throws IOException, InterruptedException {
        latch = new CountDownLatch(retrieveFileMap.size());
        for (Map.Entry<Integer, String> entry : retrieveFileMap.entrySet()) {
            logger.info("Client: Start Sending Retrieve Request To StorageNode " + entry.getValue());
            Runnable myRunnable = new MyRunnable(entry.getValue(), fileName, entry.getKey());
            Thread thread = new Thread(myRunnable);
            thread.start();
        }
        latch.await();
        logger.info("Client: Finished Sending Retrieve Request To StorageNode And Receving Data");
    }

    private static class MyRunnable implements Runnable {
        Socket socket;
        String storageNodeHostName;
        String fileName;
        int chunkId;

        public MyRunnable (String storageNodeHostName, String fileName, int chunkId) throws IOException {
            socket = new Socket(storageNodeHostName, STORAGENODE_PORT);
            this.storageNodeHostName = storageNodeHostName;
            this.fileName = fileName;
            this.chunkId = chunkId;
        }

        public void run() {
            try {
                StorageMessages.RetrieveFile.Builder retrieveFileMsg =
                        StorageMessages.RetrieveFile.newBuilder();
                retrieveFileMsg.setFileName(fileName)
                        .setChunkId(chunkId)
                        .build();
                StorageMessages.StorageMessageWrapper msgWrapper =
                        StorageMessages.StorageMessageWrapper.newBuilder()
                                .setRetrieveFileMsg(retrieveFileMsg)
                                .build();
                msgWrapper.writeDelimitedTo(socket.getOutputStream());
                logger.info("Client: Finished Sending Retrieve Request To StorageNode " + storageNodeHostName);

                logger.info("Client: Start Receving File Data From StorageNode " + storageNodeHostName);
                receiveDataFromStorageNode(socket);
                logger.info("Client: Finished Receving File Data From StorageNode " + storageNodeHostName);
                latch.countDown();
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public static void receiveDataFromStorageNode(Socket socket) throws IOException, InterruptedException {
        long currentTime = System.currentTimeMillis();
        long end = currentTime + REPLY_WAITING_TIME;
        ClientMessages.ClientMessageWrapper msgWrapper
                = ClientMessages.ClientMessageWrapper.parseDelimitedFrom(
                        socket.getInputStream()); // wait here until there is a message

        logger.info("Client: Waiting For Reply Of Retrieving From StorageNode");
        while (System.currentTimeMillis() < end) {
            if (msgWrapper.hasRetrieveFileDataMsg()) {
                ClientMessages.RetrieveFileData retrievingFileDataMsg
                        = msgWrapper.getRetrieveFileDataMsg();
                String fileName = retrievingFileDataMsg.getFileName();
                int chunkId = retrievingFileDataMsg.getChunkID();
                ByteString data =  retrievingFileDataMsg.getData();
                synchronized(retrieveChunks) {
                    retrieveChunks.add(new DFSChunk(fileName, chunkId, data));
                }
                logger.info("Client: Received Chunk: FileName: " + fileName + " ChunkId: " + chunkId);
                socket.close();
                return;
            }
            Thread.sleep(500);
        }

        socket.close();
    }

    public static void writeReceivedFileDataToLocal() throws IOException {
        if (retrieveChunks.size() > 0) {
            logger.info("Client: Start Writing Retrieved Data To Local Machine");
            Collections.sort(retrieveChunks, new ChunkComparator());
            FileOutputStream fileOutputStream = new FileOutputStream(retrieveFilePath + testRetrieveFileName);
            for (DFSChunk chunk : retrieveChunks) {
                fileOutputStream.write(chunk.getData().toByteArray());
            }
            fileOutputStream.flush();
            fileOutputStream.close();
            logger.info("Client: Finished Writing Retrieved Data To Local Machine");
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
                storeChunks.add(new DFSChunk(fileName, chunksId++, data));
            }
            logger.info("Client: Chunks Size: " + storeChunks.size());
        }
    }

    private static class ChunkComparator implements Comparator<DFSChunk> {
        public int compare(DFSChunk a, DFSChunk b) {
            return a.getChunkID() - b.getChunkID();
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
