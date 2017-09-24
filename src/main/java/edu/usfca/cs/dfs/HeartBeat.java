package edu.usfca.cs.dfs;

import java.util.HashMap;
/**
 * Created by zzc on 9/18/17.
 */
public class HeartBeat {
    public int storageNodeHostName;
    public HashMap<String, String> metaMap;
    public double freeSpace;
    public String timestamp;

    public HeartBeat(int storageNodeHostName, double freeSpace) {
        this.storageNodeHostName = storageNodeHostName;
        this.metaMap = new HashMap<String, String>();
        this.freeSpace = freeSpace;
    }
}
