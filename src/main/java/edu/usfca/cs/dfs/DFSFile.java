package edu.usfca.cs.dfs;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by zzc on 9/18/17.
 */
public class DFSFile {
    public static String filename;
    public static Map<Integer, Integer> chunkMap = new HashMap<Integer, Integer>();

    public DFSFile(String filename) {
        filename = filename;
    }

    public static void putChunk(int chunkId, int storageId) {
        chunkMap.put(chunkId, storageId);
    }
}
