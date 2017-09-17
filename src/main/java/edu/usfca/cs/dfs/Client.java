package edu.usfca.cs.dfs;


import com.google.protobuf.ByteString;

import java.io.*;
import java.net.Socket;
import java.nio.file.Files;
import java.util.logging.Logger;

public class Client {
    private static Logger logger = Logger.getLogger("Log");
    private static String filePath = "client.files/file/pig.txt";
    private static String chunkPath = "client.files/chunks";
    private static Socket socket;
    private static final int SIZEOFCHUNK = 1024 * 18;

    public static void main(String[] args) throws IOException {
        logger.info("Client: Start break file to chunks.");
        clientInit();
        logger.info("Client: Finish breaking chunks.");
    }

    public static void clientInit() throws IOException {
//      socket = new Socket("localhost", 8080);
        breakFiletoChunks(new File(filePath));
    }

    public static void sendRequestToController() throws IOException {
        ByteString data = ByteString.copyFromUtf8("Hello World!");

        StorageMessages.StoreChunk storeChunkMsg
                = StorageMessages.StoreChunk.newBuilder()
                .setFileName("my_file.txt")
                .setChunkId(3)
                .setData(data)
                .build();

        StorageMessages.StorageMessageWrapper msgWrapper =
                StorageMessages.StorageMessageWrapper.newBuilder()
                        .setStoreChunkMsg(storeChunkMsg)
                        .build();

            msgWrapper.writeDelimitedTo(socket.getOutputStream());

            socket.close();
    }

    public static void breakFiletoChunks(File file) throws IOException {
        int sizeOfChunk = SIZEOFCHUNK; // 1kB per chunk
        byte[] buffer = new byte[sizeOfChunk];
        String fileName = file.getName();
        try (FileInputStream fileInputStream = new FileInputStream(file);
             BufferedInputStream bufferedInputStream = new BufferedInputStream(fileInputStream)) {
            int bytesAmount = 0;
            int chunksId = 1;
            while ((bytesAmount = bufferedInputStream.read(buffer)) > 0) {
                String fileChunkName = String.format("%s.%05d", fileName, chunksId++);
                File chunk = new File(chunkPath, fileChunkName);
                removeChunksIfExist(chunk);
                try (FileOutputStream out = new FileOutputStream(chunk)) {
                    out.write(buffer, 0, bytesAmount);
                }
            }
        }
    }

    public static void removeChunksIfExist(File chunk) throws IOException {
        if (chunk.exists()) {
            logger.info("Client: Chunk " + chunk.getName() + " already exists, removing.");
            Files.delete(chunk.toPath());
            logger.info("Client: Chunk " + chunk.getName() + " removed.");
        }
    }

}
