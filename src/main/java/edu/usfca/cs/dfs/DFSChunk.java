package edu.usfca.cs.dfs;

import com.google.protobuf.ByteString;

/**
 * Created by zzc on 9/18/17.
 */
public class DFSChunk {
    private String filename;
    private int chunkID;
    private int storageNodeID;
    private ByteString data;

    public String getChunkName() {
        return filename;
    }
    public int getChunkID() {
        return chunkID;
    }

    public int getStorageNodeID() {
        return storageNodeID;
    }

    public ByteString getData() {
        return data;
    }

    public DFSChunk(String filename, int chunkID, ByteString data) {
        this.filename = filename;
        this.chunkID = chunkID;
        this.data = data;
    }
}
