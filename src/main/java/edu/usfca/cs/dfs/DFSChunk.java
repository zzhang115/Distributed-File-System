package edu.usfca.cs.dfs;

import com.google.protobuf.ByteString;

import java.util.Comparator;

/**
 * Created by zzc on 9/18/17.
 */
public class DFSChunk implements Comparator<DFSChunk> {
    private int chunkID;
    private int storageNodeID;
    private ByteString data;

    public int getChunkID() {
        return chunkID;
    }

    public int getStorageNodeID() {
        return storageNodeID;
    }

    public ByteString getData() {
        return data;
    }

    public DFSChunk(int chunkID) {
        this.chunkID = chunkID;
    }

    public int compare(DFSChunk a, DFSChunk b) {
        return a.chunkID - b.chunkID;
    }
}
