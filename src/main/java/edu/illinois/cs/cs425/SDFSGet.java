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
 * This thread is for inserting a file into the appropriate nodes in SDFS. 
 */
public class SDFSGet extends Thread {
    private final static Logger logger = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);
    CopyOnWriteArrayList<String> master_list;
    String localFileName;
    String sdfsFileName;

    public SDFSGet(CopyOnWriteArrayList<String> _master_list, String _localFileName, String _sdfsFileName) {
        master_list = _master_list;
        localFileName = _localFileName;
        sdfsFileName = _sdfsFileName;
    }

    /**
     * This function finds the appropriate nodes to write the file to and then writes it. 
     */
    public void run() {
        try {
            ArrayList<String> nodes = findNodesToRead();
            if (nodes.size() == 0) {
                logger.info(sdfsFileName + " does not exist in SDFS"); 
                return;
            } else {
                readFromNodes(nodes);
            }
        } catch(Exception e) {
            logger.severe(e.toString());
        }
    }

    public ArrayList<String> findNodesToRead() {
        ArrayList<String> nodesToRead = new ArrayList<String>();
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
                logger.info("Sending getWhere");
                output.writeUTF("getWhere " + sdfsFileName);
                String reply = input.readUTF();
                logger.info("Reply for getWhere " + sdfsFileName + " is " + reply);
                if (reply.equals("abort")) {
                    logger.info("File is not present in SDFS - master didn't tell where to read from"); 
                    return nodesToRead;
                }
                nodesToRead = new ArrayList<String>(Arrays.asList(reply.split(" ")));
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
        
        // TODO this is for debugging
        //nodesToWrite.add("localhost//6667//123//8888");
        return nodesToRead;
    }

    /**
     * This function will try to read the file from each node, until it gets it successfully
     */
    public boolean readFromNodes(ArrayList<String> nodes) {
        boolean success = false;
        for (String node : nodes) {
            String ip = node.split("//")[0];
            int portNumber = Integer.parseInt(node.split("//")[1]);

            Socket socket = null;
            DataInputStream dis = null;
            DataOutputStream output = null;
            FileOutputStream fos = null;

            try {
                // Open a socket to the node and read file
                socket = new Socket(ip, portNumber);
                socket.setSoTimeout(120000); // TODO check this value

                // Send the command to read, the filename
                output = new DataOutputStream(socket.getOutputStream());
                output.writeUTF("read " + sdfsFileName);
                output.flush();
                
                // Get the file
                dis = new DataInputStream(socket.getInputStream());
                fos = new FileOutputStream(localFileName);
                IOUtils.copy(dis, fos);
                fos.flush();

                // Check if the file is now present in local FS
                File file = new File(localFileName);
                if (file.exists()) {
                    if (file.isFile()) {
                        logger.info("File fetched into local file system");
                        success = true;
                    }
                }
            } catch (Exception e) {
                logger.severe(e.toString());
            } finally {
                try {
                    if (output != null)
                        output.close();
                    if (fos != null)
                        fos.close();
                    if (dis != null)
                        dis.close();
                    if (socket != null)
                        socket.close();
                    if (success) {
                        return success;
                    }
                } catch (Exception e) {
                    logger.severe(e.toString());
                }
            }
        }
        return success;
    }
}
