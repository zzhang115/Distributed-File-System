package edu.usfca.cs.dfs;

import java.util.HashMap;
/**
 * Created by zzc on 9/18/17.
 */
public class HeartBeat {
    public int storageNodeID;
    public HashMap<String, String> metaMap;
    public double freeSpace;
//    public timestamp;

    public HeartBeat(int storageNodeID, double freeSpace) {
        this.storageNodeID = storageNodeID;
        this.metaMap = new HashMap<String, String>();
        this.freeSpace = freeSpace;
    }
}
