package edu.illinois.cs.cs425;

import java.io.*;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * This thread is for handling SDFS commands entered by the user.  
 */
public class SDFSUserHandler extends Thread {
    private final static Logger logger = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);
    private boolean finished;
    CopyOnWriteArrayList<String> members;
    CopyOnWriteArrayList<String> master_list;
    CopyOnWriteArrayList<String> files;
    String command;
    String[] arguments;

    public SDFSUserHandler(CopyOnWriteArrayList<String> _members, CopyOnWriteArrayList<String> _master_list, CopyOnWriteArrayList<String> _files, String message) {
        finished = false;
        members = _members;
        master_list = _master_list;
        files = _files;
        command = message.split(" ")[0];
        arguments = Arrays.copyOfRange(message.split(" "), 1, message.split(" ").length);
    }

    /**
     * This function parses the user command and handles it appropriately. 
     */
    public void run() {
        try {
            logger.info("SDFS command entered = " + command);
            if (command.equals("put")) {
                if (arguments.length < 2) {
                    logger.info("Not enough arguments. Aborting.");
                    return;
                }
                File temp = new File(arguments[0]);
                if (temp.exists()) {
                    SDFSPut put = new SDFSPut(master_list, arguments[0], "sdfsFile/"+arguments[1]);
                    put.start();
                    put.join();
                } else {
                    logger.severe("No such file exists on local file system. Aborting.");
                }
            } else if (command.equals("get")) {
                if (arguments.length < 2) {
                    logger.info("Not enough arguments. Aborting.");
                    return;
                }
                SDFSGet get = new SDFSGet(master_list, arguments[1], "sdfsFile/"+arguments[0]);
                get.start();
                get.join();
            } else if (command.equals("delete")) {
                if (arguments.length < 1) {
                    logger.info("Not enough arguments. Aborting.");
                    return;
                }
                SDFSDelete del = new SDFSDelete(master_list, "sdfsFile/"+arguments[0]);
                del.start();
                del.join();
            } else if (command.equals("ls")) {
                String file = "";
                if (arguments.length == 0) {
                    file = "-la"; // This means ls should list all files in SDFS and their locations
                } else {
                    file = "sdfsFile/"+arguments[0];
                }
                SDFSLs ls = new SDFSLs(master_list, file);
                ls.start();
                ls.join();
            }  else if (command.equals("store")){
                File folder = new File("sdfsFile");
                File[] listOfFile = folder.listFiles();
                System.out.println("[Files stored on this node]");
                System.out.println("*************************");
                for(int i=0;i<listOfFile.length;i++){
                    System.out.println("* "+listOfFile[i].getName());
                }
                System.out.println("*************************");
                System.out.println("Total files: "+Integer.toString(listOfFile.length));
             } else {
                logger.info("Unknown SDFS command!");
            }
        } catch(Exception e) {
            logger.severe(e.toString());
        }
    }
}
