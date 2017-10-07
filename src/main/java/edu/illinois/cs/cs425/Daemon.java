package edu.illinois.cs.cs425;

import java.io.*;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.net.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.commons.cli.GnuParser; 
import org.apache.commons.cli.CommandLine; 
import org.apache.commons.cli.CommandLineParser; 
import org.apache.commons.cli.HelpFormatter; 
import org.apache.commons.cli.Option; 
import org.apache.commons.cli.Options; 
import org.apache.commons.cli.ParseException; 
import org.json.JSONArray;
import org.json.JSONObject;
import java.util.concurrent.atomic.AtomicReference;
import java.text.SimpleDateFormat;


/**
 * This class is responsible for glueing together all modules.  
 */
public class Daemon {
    private final static Logger logger = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);
    public static CopyOnWriteArrayList<String> members = new CopyOnWriteArrayList<String>();
    public static CopyOnWriteArrayList<String> master_list = new CopyOnWriteArrayList<String>();
    public static CopyOnWriteArrayList<String> files = new CopyOnWriteArrayList<String>();
    public static DatagramSocket client;
    public static boolean leader;
    public static String timeStamp;
    public static String leaderAddress;
    public static int leaderPort;
    public static InetAddress localhostName;
    public static int pingerPort ;
    public static int listenerPort;
    public static int daemonPort_temp;
    public static int sdfsPort;
    public static String path = "members.ser";
    public static boolean simulate = false; // Should packet drops in Network be simulated?
    public static int rate = 10; // Rate of Network packet drops
    public static boolean master;

    public static void init() {
        try {
            //client = new DatagramSocket(daemonPort_temp);
            leader = false;
            master = false;
            localhostName = InetAddress.getLocalHost();
            pingerPort = 6666;
            listenerPort = 6667;
            daemonPort_temp = 6665;
            sdfsPort = 6668;

            logger.finest("Default ports are: ");
            logger.finest("   daemonPort_temp:"+Integer.toString(daemonPort_temp));
            logger.finest("   pingerPort: "+Integer.toString(pingerPort));
            logger.finest("   listenerPort: "+Integer.toString(listenerPort));
    
            System.out.println(localhostName.toString());  // tobe delete
            //add log;

            //When a Daemon start from the cold place, make a new empty folder
            String commandLine = "rm -r sdfsFile";
            String commandLine1 = "mkdir sdfsFile";
            String[] command = {"/bin/sh","-c",commandLine};
            Runtime runtime = Runtime.getRuntime();
            Process process = runtime.exec(command);
            process.waitFor();
            String[] command1 = {"/bin/sh","-c",commandLine1};
            Runtime runtime1 = Runtime.getRuntime();
            Process process1 = runtime.exec(command1);
            process1.waitFor();
            logger.info("Build folder for sdfs file");

        } catch (Exception e) {
            logger.finest(e.toString());
        }
    }

    /**
     * This method is the entry point of the system which keeps all modules synchronized.
     */
    public static void main(String[] args) {
        try {
            Random rand = new Random();
            logger.setLevel(Level.INFO);
            logger.finest("Daemon started");

            init();

            // get user options
            StringBuffer sb = new StringBuffer();
            for (int i = 0;i < args.length; i++) {
                sb.append(args[i]);
                sb.append(" ");
            }
            String s = sb.toString();
            timeStamp = new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss").format(new Date());

            
            final Options options = new Options();
            final CommandLineParser parser = new GnuParser();

            Option l = new Option("l","leader",false,"declare leader");
            options.addOption(l);

            Option r = new Option("r","recover",false,"recover members list");
            options.addOption(r);

            Option a = new Option("a","address",true,"address of leader");
            options.addOption(a);

            Option p = new Option("p","port",true,"daemon client port");
            options.addOption(p);

            Option m = new Option("m","master",false,"master flag");
            options.addOption(m);

            Option p2 = new Option("p2","port of sdfs",true,"sdfs port");
            options.addOption(p2);
            
            final CommandLine commandLine = parser.parse(options,args);

            if (commandLine.hasOption("l")) {
                logger.finest("Option l is found!");
                logger.finest("Leader declared at: "+localhostName.getHostName());
                
                leader = true;
            }

            if (commandLine.hasOption("a")) {
                //TODO send join message to host
                logger.finest("Option a is found!");

                String[] leaderInfo = commandLine.getOptionValue("a").split(":");
                leaderAddress = leaderInfo[0];
                leaderPort = Integer.parseInt(leaderInfo[1]);

                logger.finest("Current leader is "+leaderAddress+":"+leaderPort); 

            }    
            
            if (commandLine.hasOption("p")) {
                logger.finest("Option p is found!");
                listenerPort = Integer.parseInt(commandLine.getOptionValue("p"));
                pingerPort = listenerPort-1;
                daemonPort_temp = listenerPort-2;


                logger.finest("Port specified: "+Integer.toString(daemonPort_temp)+"\n"+
                    "Set daemonPort at "+Integer.toString(daemonPort_temp)+"\n"+
                    "Set listenerPort at "+Integer.toString(listenerPort)+"\n"+
                    "Set pingerPort at "+Integer.toString(pingerPort)+"\n");
            }
            if(commandLine.hasOption("p2")){
                logger.finest("Option p2 is found!");
                sdfsPort = Integer.parseInt(commandLine.getOptionValue("p2"));
            }

            if(commandLine.hasOption("m")){
                logger.info("User specified master");
                master = true;
                // leader will add itself as the first master in the master list
                if(leader==true){
                    String master_ID = localhostName.getHostName()+"//"+Integer.toString(listenerPort)+"//"+timeStamp+"//"+Integer.toString(sdfsPort);
                    master_list.add(master_ID);
                }  
                
            }

            if (commandLine.hasOption("r")) {
                logger.finest("Option r is found!");
                readFromBackup();
                client = new DatagramSocket(daemonPort_temp);
                // TODO Send the new member list a broadcast that the leader is alive again - a join message
                // With the new ID this time
                String newleader = localhostName.getHostName()+"//"+Integer.toString(listenerPort)+"//"+timeStamp+"//"+Integer.toString(sdfsPort);
                broadCast("join",newleader);
        logger.finest("Broad cast new leader!");
            } 

            logger.finest("Daemon start a listener: "+localhostName.getHostName()+":"+Integer.toString(listenerPort));
            Listener listener = new Listener(listenerPort,members,master_list,files,leader,timeStamp,sdfsPort);
            listener.start();
        
            logger.finest("Daemon start a pinger: "+localhostName.getHostName()+":"+Integer.toString(pingerPort));
            Pinger pinger = new Pinger(members, master_list,timeStamp,pingerPort,sdfsPort);
            pinger.start();

            logger.finest("Starting SDFS Listener: "+localhostName.getHostName()+":"+Integer.toString(sdfsPort));
            SDFSListener sl = new SDFSListener(sdfsPort, members, master_list, files);
            sl.start();

           
            Backup backup = new Backup(members, path);

            if (leader == false) {
                //initial the memberlist by sending the first "join" message to leader's listener. leader will send back a current memberlist to local listener 
        client = new DatagramSocket(daemonPort_temp);

                logger.finest("Start an UDP client for initialize!");

                InetAddress leaderIP = InetAddress.getByName(leaderAddress);
                JSONObject jsonArgument = new JSONObject();
                String message = localhostName.getHostName()+"//"+Integer.toString(listenerPort)+"//"+timeStamp+"//"+Integer.toString(sdfsPort);
                jsonArgument.put("join", message);
                if(master==true){
                    jsonArgument.put("master","true"); //attached the master status in the join message.
                } 
                byte[] sendData = jsonArgument.toString().getBytes();
                DatagramPacket sendPacket = new DatagramPacket(sendData,sendData.length,leaderIP,leaderPort);
                client.send(sendPacket);

                logger.finest("request member list from leader "+leaderAddress.toString()+Integer.toString(leaderPort));

                //client.close();     //TODO should close when leave      

                //logger.finest("Close client");
            } else {
                if (client == null) {
                    client = new DatagramSocket(daemonPort_temp);
                }
                backup.start();
            }

            // UserHandler
            logger.finest("User Handler starting!");
            
            Scanner scanner = new Scanner(System.in);
            String message = "";
            
            while (true) {
                message = scanner.nextLine();
                if (message.equals("l") || message.equals("leave")) {
                    logger.finest("Leave command issued by user.");

                    // TODO Broadcast message to all members here, 3 times
                    JSONObject jsonBroadcast = new JSONObject();
                    String myID = localhostName.getHostName()+"//"+Integer.toString(listenerPort)+"//"+timeStamp+"//"+Integer.toString(sdfsPort);
                    jsonBroadcast.put("leave",myID);
                    for (int i = 0; i < 3; i++) {
                        logger.finest("Broadcast round: "+Integer.toString(i));
                        Iterator<String> it = members.iterator();        
                        while (it.hasNext()) {      
                            String memberInfo = it.next();
                            byte[] sendBroadcast = jsonBroadcast.toString().getBytes();
                            InetAddress memberIP = InetAddress.getByName(memberInfo.split("//")[0]);
                            String memberPort = memberInfo.split("//")[1];
                            DatagramPacket broadcastPacket = new DatagramPacket(sendBroadcast,sendBroadcast.length,memberIP,Integer.parseInt(memberPort));
                            
                            if (Daemon.simulate) {
                                int randNum = rand.nextInt(100);
                                if (randNum <= Daemon.rate) {
                                    logger.finest("Dropping packet");
                                } else {
                                    client.send(broadcastPacket); // Ping is sent
                                }
                            } else {
                                client.send(broadcastPacket); // Ping is sent
                            }

                            logger.finest("Leave message broadcast to "+memberInfo);
                        }
                        //Sleep 100ms before start next broadcast round
                        Thread.sleep(100);  
                    }
                    if (members.size() == 0) {
                        logger.finest("Empty member list - quitting!");
                    }
                    pinger.stopMe();
                    listener.stopMe();
                    if (leader == true) {
                        backup.stopMe();
                    }
                    client.close();
                    logger.finest("Quitting - All threads stopped!");
                    return;                    
                } else if (message.equals("p") || message.equals("print")) {
                    printMemberlist();
                } else if (message.equals("i") || message.equals("id")) {
                    logger.finest("This process has ID:");
                    System.out.println("*******************************");
                    if(master == true){
                        System.out.println("M:"+localhostName.getHostName()+"//"+Integer.toString(listenerPort)+"//"+timeStamp+"//"+Integer.toString(sdfsPort));
                    }else{
                        System.out.println("  "+localhostName.getHostName()+"//"+Integer.toString(listenerPort)+"//"+timeStamp+"//"+Integer.toString(sdfsPort));
                    }
                    System.out.println("*******************************");
                } else if (message.equals("t") || message.equals("toggle")) {
                    // This is for toggling the value of "simulate" variable for simulating packet drops
                    logger.finest("Toggling the value of simulate");
                    simulate = !simulate;
                    logger.finest("New value of simulate is " + simulate);
                } else if(message.equals("pm")){
                    logger.info("Current master list");
                    Iterator<String> it = master_list.iterator();
                    System.out.println("**************************");
                    while(it.hasNext()){
                        System.out.println("* "+it.next());
                    }
                    System.out.println("*******************************");
                    System.out.println("*Total masters: "+ Integer.toString(master_list.size()));

                } else if(message.equals("pf")){
                    Iterator<String> it = files.iterator();
                    System.out.println("[File log]");
                    System.out.println("**************************");
                    while(it.hasNext()){
                        System.out.println("* "+it.next());
                    }
                    System.out.println("*******************************");
                }else {
                    SDFSUserHandler su = new SDFSUserHandler(members, master_list, files, message);
                    su.start();
                }
            }

            //pinger.join(); // is it correct?? don't know
            //listener.join();
        } catch (Exception e) {
            logger.finest(e.toString());
        }
    }

    /** 
    * This function is to print current member list to screen
    */
    public static void printMemberlist() {
        try{
            logger.finest("Current member list is:");
            Iterator<String> it = members.iterator();
            System.out.println("*******************************");
            int i=0;
            String selfID = InetAddress.getLocalHost().getHostName()+"//"+Integer.toString(listenerPort)+"//"+timeStamp+"//"+Integer.toString(sdfsPort);
            if(!master_list.contains(selfID)){
                System.out.println("*"+" "+selfID);
            }else{
                System.out.println("*"+"M:"+selfID);
            }
            while (it.hasNext()) {
                  String member = it.next();
                  if(!master_list.contains(member)){
                    System.out.println("*"+" "+member);
                }else{
                    System.out.println("*"+"M:"+member);
                }
                  
                  i++;
            }
            System.out.println("*Total members: "+ Integer.toString(i+1));
            System.out.println("*Total masters: "+ Integer.toString(master_list.size()));
            System.out.println("*******************************");            
        }catch(Exception e){
            logger.finest(e.toString());
        }

    }

    /**
     * This function is to broadcast to all member in the list
     */
    public static void broadCast(String keyWord,String content) {
        try {
            Iterator<String> it = members.iterator();
            JSONObject jsonBroadcast = new JSONObject();
            jsonBroadcast.put(keyWord,content);
            while (it.hasNext()) {      
                String member = it.next();
                byte[] sendBroadcast = jsonBroadcast.toString().getBytes();
                InetAddress memberIP = InetAddress.getByName(member.split("//")[0]);
                String memberPort = member.split("//")[1];
                DatagramPacket broadcastPacket = new DatagramPacket(sendBroadcast,sendBroadcast.length,memberIP,Integer.parseInt(memberPort));
                client.send(broadcastPacket);
            }
        } catch(Exception e) {
            logger.finest(e.toString());
        }
    }
    /** 
     * This function is to read the members list from backup if starting a node
     */
    public static void readFromBackup() {
        logger.finest("Recovering from backup file " + path);
        try {
            FileInputStream fileIn = new FileInputStream(path);
            ObjectInputStream in = new ObjectInputStream(fileIn);
            members = (CopyOnWriteArrayList<String>) in.readObject();
            in.close();
            fileIn.close();
            logger.finest("Recovered members list successfully!");
        } catch(IOException i) {
            logger.finest(i.toString());
            return;
        } catch(ClassNotFoundException c) {
            logger.finest(c.toString());
            return;
        }
    }
}
