package edu.usfca.cs.dfs;

import com.google.protobuf.ByteString;

import java.util.Scanner;

/**
 * Created by zzc on 9/18/17.
 */
public class DFSChunk {
    private String filename;
    private int chunkID;
    private ByteString data;
    private long chunkSize;

    public String getChunkName() {
        return filename;
    }

    public int getChunkID() {
        return chunkID;
    }

    public ByteString getData() {
        return data;
    }

    public long getChunkSize() {
        return chunkSize;
    }

    public DFSChunk(String filename, int chunkID, ByteString data) {
        this.filename = filename;
        this.chunkID = chunkID;
        this.data = data;
        this.chunkSize = data.size();
    }
}
