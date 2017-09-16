package edu.usfca.cs.dfs;


import java.io.IOException;
import java.net.Socket;
import java.util.logging.Logger;

public class Client {
    private static Logger logger = Logger.getLogger("log");

    public static void main(String[] args) throws IOException {
        logger.info("first log info");
    }

    public static void init() throws IOException {
        Socket socket = new Socket("localhost", 8080);

    }
}
