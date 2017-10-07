package edu.illinois.cs.cs425;

import java.io.*;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * This thread is for periodically backing up the member list for the leader.
 */
public class Backup extends Thread {
    private final static Logger logger = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);
    CopyOnWriteArrayList<String> members;
    boolean finished;
    String path;

    public Backup(CopyOnWriteArrayList<String> _members, String _path) {
        members = _members;
        path = _path;
    }
    
    /**
     * This function is used to gracefully stop this thread if needed.
     */
    public void stopMe() {
        logger.finest("Stopping");
        finished = true;
        logger.finest("Stopped");
    }

    /**
     * This function periodically writes the members list to serialized object.
     */
    public void run() {
        while (!Thread.currentThread().isInterrupted() && !finished) {
            try {
                logger.finest("Making backup");
                FileOutputStream fileOut = new FileOutputStream(path);
                ObjectOutputStream out = new ObjectOutputStream(fileOut);
                out.writeObject(members);
                out.close();
                fileOut.close();
                logger.finest("Serialized data is saved in " + path);
                Thread.sleep(1000);
                logger.finest("Backup made.");
            } catch(Exception e) {
                logger.severe(e.toString());
            }
        }
    }
}
