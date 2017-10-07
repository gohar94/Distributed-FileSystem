package edu.illinois.cs.cs425;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import org.apache.commons.io.IOUtils;

/**
 * This thread is for writing a local file to an SDFS node. 
 */
public class SDFSWriter extends Thread {
    private final static Logger logger = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);
    CopyOnWriteArrayList<String> master_list;
    String localFileName;
    String sdfsFileName;
    String node;
    CountDownLatch doneSignal;
    Socket socket;
    DataOutputStream output;
    FileInputStream fis;
    boolean success;

    public SDFSWriter(CopyOnWriteArrayList<String> _master_list, String _node, String _localFileName, String _sdfsFileName, CountDownLatch _doneSignal) {
        master_list = _master_list;
        node = _node;
        localFileName = _localFileName;
        sdfsFileName = _sdfsFileName;
        doneSignal = _doneSignal;
        output = null;
        fis = null;
        success = false;
        socket = new Socket();
    }
    
    /**
     * This function sends the local file to the node and waits for an ACK after its written. 
     */
    public void run() {
        try {
            String ip = node.split("//")[0];
            int portNumber = Integer.parseInt(node.split("//")[3]);
            // int portNumber = 8888; // TODO this should be removed and should use above line

            // Open a socket to the node and write file
            socket = new Socket(ip, portNumber);
            socket.setSoTimeout(120000); // TODO check this value

            // Send the command to write, the filename
            output = new DataOutputStream(socket.getOutputStream());
            output.writeUTF("write " + sdfsFileName);
            output.flush();
            
            // Send the file
            fis = new FileInputStream(localFileName);
            IOUtils.copy(fis, output);
            output.flush();

            success = true;
        } catch(Exception e) {
            logger.severe(e.toString());
        } finally {
            try {
                fis.close();
                output.close();
                socket.close();
                logger.info("File " + localFileName + " sent as " + sdfsFileName);
                if (success)
                    doneSignal.countDown();
                else
                    logger.severe("Not successful in writing!");
            } catch (Exception e) {
                logger.severe(e.toString());
            }
        }
    }
}
