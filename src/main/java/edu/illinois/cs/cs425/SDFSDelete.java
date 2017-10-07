package edu.illinois.cs.cs425;

import java.io.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.net.*;
import org.apache.commons.io.IOUtils;

/**
 * This thread is for deleting a file from SDFS 
 */
public class SDFSDelete extends Thread {
    private final static Logger logger = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);
    CopyOnWriteArrayList<String> master_list;
    String sdfsFileName;

    public SDFSDelete(CopyOnWriteArrayList<String> _master_list, String _sdfsFileName) {
        master_list = _master_list;
        sdfsFileName = _sdfsFileName;
    }

    /**
     * This function finds the appropriate nodes to delete the file from and then deletes it. 
     */
    public void run() {
        try {
            ArrayList<String> nodes = findNodesToDelete();
            if (nodes.size() == 0) {
                logger.info(sdfsFileName + " does not exist in SDFS"); 
                return;
            } else {
                deleteFromNodes(nodes);
            }
        } catch(Exception e) {
            logger.severe(e.toString());
        }
    }

    public ArrayList<String> findNodesToDelete() {
        ArrayList<String> nodesToDelete = new ArrayList<String>();
        // Contact each master serially until one replies (get message)
        // TODO this below chunk is for local debugging, instead send message to all masters
        Socket socket = null;
        DataInputStream input = null;
        DataOutputStream output = null;
        try {
            for (String str : master_list) {
                String[] info = str.split("//");
                String ip = info[0];
                int portNumber = Integer.parseInt(info[3]);
                logger.info("JBTW master is " + str);
                socket = new Socket(ip, portNumber);
                input = new DataInputStream(socket.getInputStream());
                output = new DataOutputStream(socket.getOutputStream());
                logger.info("Sending getWhereD");
                output.writeUTF("getWhereD " + sdfsFileName);
                String reply = input.readUTF();
                logger.info("Reply for getWhereD " + sdfsFileName + " is " + reply);
                if (reply.equals("abort")) {
                    logger.info("File is not present in SDFS - master didn't tell where to delete from"); 
                    return nodesToDelete;
                }
                nodesToDelete = new ArrayList<String>(Arrays.asList(reply.split(" ")));
            }
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
            } catch (Exception e) {
                logger.severe(e.toString());
            }
        }
        
        return nodesToDelete;
    }

    /**
     * This function will try to read the file from each node, until it gets it successfully
     */
    public boolean deleteFromNodes(ArrayList<String> nodes) {
        boolean success = true;
        for (String node : nodes) {
            String ip = node.split("//")[0];
            int portNumber = Integer.parseInt(node.split("//")[1]);

            Socket socket = null;
            DataOutputStream output = null;

            try {
                // Open a socket to the node and read file
                socket = new Socket(ip, portNumber);
                socket.setSoTimeout(120000); // TODO check this value

                // Send the command to read, the filename
                output = new DataOutputStream(socket.getOutputStream());
                output.writeUTF("delete " + sdfsFileName);
                output.flush();
            } catch (Exception e) {
                success = false;
                logger.severe("Current exception thrown by node " + node);
                logger.severe(e.toString());
            } finally {
                try {
                    if (output != null)
                        output.close();
                    if (socket != null)
                        socket.close();
                } catch (Exception e) {
                    success = false;
                    logger.severe("Current exception thrown by node " + node);
                    logger.severe(e.toString());
                }
            }
        }
        return success;
    }
}
