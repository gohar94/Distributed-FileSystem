package edu.illinois.cs.cs425;

import java.io.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.net.*;

/**
 * This thread is for inserting a file into the appropriate nodes in SDFS. 
 */
public class SDFSPut extends Thread {
    private final static Logger logger = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);
    CopyOnWriteArrayList<String> master_list;
    String localFileName;
    String sdfsFileName;

    public SDFSPut(CopyOnWriteArrayList<String> _master_list, String _localFileName, String _sdfsFileName) {
        master_list = _master_list;
        localFileName = _localFileName;
        sdfsFileName = _sdfsFileName;
    }

    /**
     * This function finds the appropriate nodes to write the file to and then writes it. 
     */
    public void run() {
        try {
            // TODO if all replicas did not successfully write the file, then take action!
            ArrayList<String> nodes = findNodesToWrite();
            if (nodes.size() == 0) {
                logger.info("Not writing to any nodes - finishing this thread!");
                return;
            }
            int successfulReplicas = writeToNodes(nodes);
            logger.info(sdfsFileName + " written to " + successfulReplicas + " nodes");
            if (successfulReplicas == Math.min(nodes.size(), 3)) {
                logger.info("All replicas have written the file " + sdfsFileName);
            }
        } catch(Exception e) {
            logger.severe(e.toString());
        }
    }

    /**
     * This function contacts the master(s) to find the appropriate nodes to write this file to.
     */
    public ArrayList<String> findNodesToWrite() {
        ArrayList<String> nodesToWrite = new ArrayList<String>();
        // Contact each master serially until one replies (putWhere message)
        
        boolean success = false;
        for (String str : master_list) {
            Socket socket = null;
            DataInputStream input = null;
            DataOutputStream output = null;
            try {
                logger.info("Trying master " + str);
                socket = new Socket(str.split("//")[0],Integer.parseInt(str.split("//")[3]));
                input = new DataInputStream(socket.getInputStream());
                output = new DataOutputStream(socket.getOutputStream());
                logger.info("Sending putWhere");
                output.writeUTF("putWhere " + sdfsFileName);
                String reply = input.readUTF();
                logger.info("Reply for putWhere " + sdfsFileName + " is " + reply);
                if (reply.equals("abort")) {
                    logger.info("File is already in SDFS - master did not tell where to write!");
                    return nodesToWrite;
                }
                nodesToWrite = new ArrayList<String>(Arrays.asList(reply.split(" ")));
                success = true;
            } catch (Exception e) {
                logger.severe(e.toString());
            } finally {
                try {
                    if (input != null)
                        input.close();
                    if (output != null)
                        output.close();
                    if (socket != null)
                        socket.close();
                    if (success) {
                        break;
                    }
                } catch (Exception e) {
                    logger.severe(e.toString());
                }
            }
        }
        
        return nodesToWrite;
    }

    /**
     * Given a list of N members, this function spawns N threads to write the file in parallel.
     */
    public int writeToNodes(ArrayList<String> nodes) {
        CountDownLatch doneSignal = new CountDownLatch(nodes.size());
        // Iterate over the nodes, spawn a writer thread for each
        for (String node : nodes) {
            logger.info("Writing on node " + node);
            SDFSWriter sw = new SDFSWriter(master_list, node, localFileName, sdfsFileName, doneSignal);
            sw.start();
        }

        try {
            doneSignal.await(100, TimeUnit.SECONDS); // TODO timeout check
        } catch (Exception e) {
            logger.severe(e.toString());
        }

        return nodes.size() - (int)doneSignal.getCount(); // Number of nodes successfully written to
    } 
}
