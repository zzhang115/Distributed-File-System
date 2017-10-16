package edu.usfca.cs.dfs;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Created by zzc on 10/15/17.
 */

public class CheckSum {
    public static String fileCheckSum(String fileName) throws IOException {
        File file = new File(fileName);
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
