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
 * This thread is for executing ls command 
 */
public class SDFSLs extends Thread {
    private final static Logger logger = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);
    CopyOnWriteArrayList<String> master_list;
    String fileName;

    public SDFSLs(CopyOnWriteArrayList<String> _master_list, String _fileName) {
        master_list = _master_list;
        fileName = _fileName;
    }

    /**
     * This function finds the appropriate nodes to write the file to and then writes it. 
     */
    public void run() {
        try {
            getLsFromMasters();
            logger.info("Master size is " + master_list.size());
        } catch(Exception e) {
            logger.severe(e.toString());
        }
    }

    /**
     * This function will send the ls command to masters serially until one replies 
     */
    public boolean getLsFromMasters() {
        boolean success = false;
        for (String master : master_list) {
            DataOutputStream output = null; 
            DataInputStream input = null;
            Socket socket = null;
            try {
                String[] info = master.split("//");
                String ip = info[0];
                int port = Integer.parseInt(info[3]);
                socket = new Socket(info[0], port);
                output = new DataOutputStream(socket.getOutputStream());
                input = new DataInputStream(socket.getInputStream());
                output.writeUTF("ls " + fileName); 
                String response = input.readUTF();
                logger.info("ls response is:");
                // logger.info(response);
                String[] list = response.split(" ");
                System.out.println("*********************");
                for (String temp : list) {
                    System.out.println(temp);
                }
                System.out.println("*********************");
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
