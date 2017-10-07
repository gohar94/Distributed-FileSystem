package edu.illinois.cs.cs425;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * This thread is for listening to SDFS messages from other nodes. 
 */
public class SDFSListener extends Thread {
    private final static Logger logger = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);
    private boolean finished;
    CopyOnWriteArrayList<String> master_list;
    CopyOnWriteArrayList<String> members;
    CopyOnWriteArrayList<String> files;
    ServerSocket listener;

    public SDFSListener(int portNumber, CopyOnWriteArrayList<String> _members, CopyOnWriteArrayList<String> _master_list, CopyOnWriteArrayList<String> _files) { 
        try {
            listener = new ServerSocket(portNumber);
        } catch (Exception e) {
            logger.info("SDFS Lisener could not be started");
            logger.severe(e.toString());
            return;
        }
        finished = false;
        master_list = _master_list;
        members = _members;
        files = _files;
    }
    
    /**
     * This function is used to gracefully stop this thread if needed.
     */
    public void stopMe() {
        logger.info("Stopping");
        finished = true;
        logger.info("Stopped");
    }

    /**
     * This function parses the messages from SDFS nodes and acts accordingly.  
     */
    public void run() {
        try {
            if (listener == null) {
                logger.severe("SDFS Listener is not listening - killing this thread");
                return;
            }

            logger.info("SDFS Listener started listening on " + listener.getInetAddress() + ":" + listener.getLocalPort());

            while (!Thread.currentThread().isInterrupted() && !finished) {
                Socket socket = listener.accept();
                logger.info("Connected to " + socket.getRemoteSocketAddress());
                
                logger.info("Spawning thread to handle incoming request to SDFS Listener");
                SDFSMessageHandler handler = new SDFSMessageHandler(socket, members, master_list, files);
                handler.start();
            }
        } catch(Exception e) {
            logger.info("Problem in SDFS Listener thread - killing this thread");
            logger.severe(e.toString());
        }
    }
}
