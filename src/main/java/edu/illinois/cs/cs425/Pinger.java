package edu.illinois.cs.cs425;

import java.io.*;
import java.util.*;
import org.json.JSONArray;
import org.json.JSONObject;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.Random;
import java.util.concurrent.CopyOnWriteArrayList;
import java.net.*;

/**
 * This class randomly selects a ping target periodically and checks if it is alive - if not, it takes appropriate action.
 */
public class Pinger extends Thread {
    private final static Logger logger = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);
    private boolean finished;
    private Random rand;
    private int port;
    private int sdfsPort;
    private String timestamp;
    CopyOnWriteArrayList<String> members;
    CopyOnWriteArrayList<String> master_list;

    /**
     * This constructs a Pinger thread given a membership list.
     */
    public Pinger(CopyOnWriteArrayList<String> _members,CopyOnWriteArrayList<String> _master_list, String _timestamp,int _port,int _sdfsPort) {
        members = _members;
        master_list = _master_list;
        finished = false;
        port = _port;
        timestamp = _timestamp;
        sdfsPort = _sdfsPort;
        rand = new Random();
    }

    /**
     * This function is used to inform SDFS a perticular member is down
     */
    public void informMaster(String target){
        try{
            String topMaster = master_list.get(0);
            String master_Ip = topMaster.split("//")[0];
            int master_Port = Integer.parseInt(topMaster.split("//")[3]);
            Socket sdfsClient = new Socket(master_Ip,master_Port);
            DataOutputStream dos = new DataOutputStream(sdfsClient.getOutputStream());
            dos.writeUTF("manDown " + target);
            dos.flush();
            logger.info("Informed master: "+topMaster+"[Failure of "+target+"]");
            sdfsClient.close();
            dos.close();
        }catch(Exception e){
            logger.info(e.toString());
        }
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
     * This function is used to print current memberlist
     */
    public void printMemberlist()
      {
        try{

            Iterator<String> it = members.iterator();
            logger.finest("Current memberlist is:");
            System.out.println("*******************************");
            int i=0;
            String selfID = InetAddress.getLocalHost().getHostName()+"//"+Integer.toString(port+1)+"//"+timestamp+"//"+Integer.toString(sdfsPort);
            if(!master_list.contains(selfID)){
                System.out.println("*"+" "+selfID);
            }else{
                System.out.println("*"+"M:"+selfID);
            }
            while(it.hasNext()){
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
     * This function chooses a random ping target and pings it.
     */
    public void run() {
        try {
            String member = null;
            int index;
            while (!Thread.currentThread().isInterrupted() && !finished) {
                if (members.size() != 0) {
                    index = rand.nextInt(members.size());
                    logger.finest("Randomly choosing index " + index + " from members list");
                    member = members.get(index);

                    // This may happen when the membership list is empty
                    if (member == null) {
                        continue;
                    }

                    pingMember(member, index);
                } else {
                    logger.finest("No members to choose from!");
                }

                // Time until next ping.
                logger.finest("Going to sleep!");
                Thread.sleep(750); // TODO Fix this value later, changing for debugging
                logger.finest("Just woke up!");
            }
        } catch (Exception e) {
            logger.severe("Thread interrupted");
            logger.severe(e.toString());
        }
        logger.finest("Quitting Pinger");
    }

    /**
     * This function pings one member, if no ACK is received it will timeout and mark it as suspicios.
     */
    public void pingMember(String member, int index) {
        logger.finest("PingMember for " + member + " with index " + index);
        try {
            String[] parts = member.split("//");
            String serverHostname = parts[0];
            int dstPort = Integer.parseInt(parts[1]); // Every daemon listener will be started on this port 
            int srcPort = port;
            DatagramSocket clientSocket = new DatagramSocket(srcPort);
            InetAddress IPAddress = InetAddress.getByName(serverHostname);
            byte[] sendData;
            byte[] receiveData = new byte[2048];
            String seqNum = Integer.toString(rand.nextInt(1000));

            JSONObject pingObj = new JSONObject();
            pingObj.put("ping", seqNum);
            pingObj.put("id", parts[2]); // This is the unique ID of the node we are pinging
            pingObj.put("srcIp", InetAddress.getLocalHost().getHostAddress());
            pingObj.put("srcPort", Integer.toString(srcPort));
            
            String ping = pingObj.toString();
            sendData = ping.getBytes();

            logger.finest("Connecting to " + IPAddress.toString() + " via UDP port " + dstPort);
            logger.finest("Sending " + sendData.length + " bytes to server");
            logger.finest("Sending packet = " + ping);
            DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, IPAddress, dstPort); 

            if (Daemon.simulate == true) {
                int randNum = rand.nextInt(100);
                if (randNum <= Daemon.rate) {
                    logger.severe("Dropping packet");
                } else {
                    clientSocket.send(sendPacket); // Ping is sent
                }
            } else {
                clientSocket.send(sendPacket); // Ping is sent
            }

            logger.finest("Sent " + sendData.length + " bytes to server");

            DatagramPacket receivePacket = new DatagramPacket(receiveData, receiveData.length);
            logger.finest ("Waiting for response packet");
            //Set the direct ping timeout to 100ms
            clientSocket.setSoTimeout(100); // Timeout period to wait for ACK 100ms - otherwise mark member as suspicious

            try {
                // Waiting for an ACK
                clientSocket.receive(receivePacket); 
                String response = new String(receivePacket.getData()); 
                InetAddress returnIPAddress = receivePacket.getAddress();
                int responsePort = receivePacket.getPort();
                logger.finest("Response from server at: " + returnIPAddress + ":" + responsePort);
                logger.finest("Response message so far: " + response);
                clientSocket.close(); 
            } catch (Exception e) {
                // Member did not send back an ACK
                logger.finest("Timeout occured, packet maybe lost! SeqNum = " + seqNum);
                logger.finest("Going to PingReq for " + member);
                pingReq(member, index, clientSocket, srcPort); // This function and any of its child functions are not allowed to close the socket
                logger.finest("Going to close clientSocket");
                // logger.severe(e.toString()); // TODO Delete this later
                clientSocket.close();
            }
        } catch (Exception e) {
            // Could not send a message to the member
            logger.severe(e.toString());
        }
    }

    /**
     * This function is to start a ping req for a potentially failed node.
     */
    public void pingReq(String target, int index, DatagramSocket clientSocket, int srcPort) {
        logger.finest("Starting PingReq for " + target);
        String[] temp = new String[members.size()];
        temp = members.toArray(temp);
        ArrayList<String> copy = new ArrayList<String>(Arrays.asList(temp)); // Copy the array to remove member from it
        //check if the member is still in the memberlist
        if(copy.contains(target))
        {
            copy.remove(index); // Remove the member so it doesn't get selected as pingreq target
            ArrayList<String> kMembers = new ArrayList<String>(); // These are the k members to send pingreq to

            // Pick min(k, copy.size()) members to send pingreq
            int k = Math.min(3, copy.size());
            String mem = null;
            for (int i = 0; i < k; i++) {
                if (copy.size() == 0) {
                    break;
                }

                index = rand.nextInt(copy.size());
                mem = copy.get(index);

                if (mem == null) {
                    break;
                }
                
                kMembers.add(mem);
                logger.finest("Picked " + mem + " (" + index + "th member) for K pingreq members");
                k--;
                copy.remove(index);
            }

            boolean success = false;
            int retries = 3;
            
            if (kMembers.size() > 0) {
                for (int i = 0; i < retries; i++) {
                    logger.finest("Trying for " + i + "th time");
                    if (pingReqKMembers(target, kMembers, clientSocket, srcPort)) {
                        success = true;
                        break;
                    }
                }
            }

            if (!success) {
                // TODO Broadcast a failure message for target
                logger.finest("Could not reach target - marking as failed!");
                if(master_list.contains(target)){
                    master_list.remove(target);
                    logger.finest("Removed master "+target);
                }
                if (members.contains(target)) {
                    boolean status = members.remove(target); 
                    logger.finest("Status of remove command = " + status);
                    logger.severe("Removed member " + target);
                    printMemberlist();
                    for (int i = 0; i < 3; i++) {
                        logger.finest("Start broadcast round "+Integer.toString(i+1));
                        try {
                            Iterator<String> it = members.iterator();
                            logger.finest("Failure broadcast: member "+target+" has failed");
                            JSONObject jsonBroadcast = new JSONObject();
                            jsonBroadcast.put("fail",target);
                            jsonBroadcast.put("srcIp",InetAddress.getLocalHost().getHostAddress());
                            jsonBroadcast.put("srcPort",Integer.toString(srcPort));
                            while (it.hasNext()) {           
                                String memberInfo = it.next();
                                byte[] sendBroadcast = jsonBroadcast.toString().getBytes();
                                InetAddress memberIP = InetAddress.getByName(memberInfo.split("//")[0]);
                                String memberPort = memberInfo.split("//")[1];
                                DatagramPacket broadcastPacket = new DatagramPacket(sendBroadcast,sendBroadcast.length,memberIP,Integer.parseInt(memberPort));
                                
                                if (Daemon.simulate == true) {
                                    int randNum = rand.nextInt(100);
                                    if (randNum <= Daemon.rate) {
                                        logger.severe("Dropping packet");
                                    } else {
                                        clientSocket.send(broadcastPacket); // Ping is sent
                                    }
                                } else {
                                    clientSocket.send(broadcastPacket); // Ping is sent
                                }
                                logger.finest("Failure message broadcast to member "+memberInfo);
                                
                            }
                            logger.info("Detect failure! Inform Master!");
                            
                            Thread.sleep(20);
                        } catch (Exception e) {
                            logger.finest("Failure message broadcast error!");
                            logger.finest(e.toString());
                        }
                    }
                    informMaster(target);
                }
                else {
                    logger.finest("member "+target+" alreadry declared as failed");
                }                
            } else {
                logger.severe("PingReq finished - member is alive!");
            }
        }
        else {
            logger.finest("Target already marked down! no more pingreq!");
        }
        logger.finest("Going out of pingReq");
    }
    
    /**
     * This function is to relay a ping req using K selected members.
     */
    public boolean pingReqKMembers(String target, ArrayList<String> kMembers, DatagramSocket clientSocket, int srcPort) {
        logger.severe("Starting PingReq with K selected members for target " + target);
        try {
            ArrayList<String> validSeqs = new ArrayList<String>(); // List of sequence numbers that have been sent

            // Send a pingReq to all K members
            for (String member : kMembers) {
                String[] parts = member.split("//");
                String serverHostname = parts[0];
                int dstPort = Integer.parseInt(parts[1]); // Every daemon listener will be started on this port 
                InetAddress IPAddress = InetAddress.getByName(serverHostname);
                byte[] sendData;
                String seqNum = Integer.toString(rand.nextInt(1000));
                validSeqs.add(seqNum);

                JSONObject pingObj = new JSONObject();
                pingObj.put("pingReq", seqNum);
                pingObj.put("target", target);
                pingObj.put("id", target.split("//")[2]); // This is the unique ID of the node we are pinging
                pingObj.put("srcIp", InetAddress.getLocalHost().getHostAddress());
                pingObj.put("srcPort", Integer.toString(srcPort));
                
                String ping = pingObj.toString();
                sendData = ping.getBytes();

                logger.finest("Connecting to " + IPAddress.toString() + " via UDP port " + dstPort);
                logger.finest("Sending " + sendData.length + " bytes to server");
                logger.finest("Sending packet = " + pingObj);
                DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, IPAddress, dstPort); 
                
                if (Daemon.simulate == true) {
                    int randNum = rand.nextInt(100);
                    if (randNum <= Daemon.rate) {
                        logger.severe("Dropping packet");
                    } else {
                        clientSocket.send(sendPacket); // Ping is sent
                    }
                } else {
                    clientSocket.send(sendPacket); // Ping is sent
                }

                logger.finest("Sent " + sendData.length + " bytes to server");
            }

            // Wait for any one of them to send back an ACK response
            // As soon as the firs ACK is received, we exit from this
            byte[] receiveData = new byte[2048]; // Assuming that the response will not be greater than this for ACK here
            DatagramPacket receivePacket = new DatagramPacket(receiveData, receiveData.length);
            logger.severe("Waiting for response packet");
            clientSocket.setSoTimeout(200); // Timeout period to wait for ACK - otherwise mark member as suspicious

            try {
                // Waiting for an ACK with valid sequence number
                while (true) {
                    clientSocket.receive(receivePacket); 
                    String response = new String(receivePacket.getData()); 
                    JSONObject responseObj = new JSONObject(response);

                    InetAddress returnIPAddress = receivePacket.getAddress();
                    int responsePort = receivePacket.getPort();

                    logger.finest("Response from server at: " + returnIPAddress + ":" + responsePort);
                    logger.finest("Response message for pingReq: " + response);

                    if (responseObj.has("ack")) {
                        if (!validSeqs.contains(responseObj.getString("ack"))) {
                            // If a valid sequence number is not received (some stale packet) 
                            // then we keep listening until timeout period expires
                            logger.finest("ACK received but with an invalid sequence number " + responseObj.getString("ack") + " - still waiting!");
                            continue;
                        } else {
                            logger.finest("ACK received with valid sequence number " + responseObj.getString("ack"));
                            break;
                        }
                    }
                }
                return true;
            } catch (Exception e) {
                // None of the k members sent back an ACK
                // logger.severe(e.toString());
                logger.severe("Timeout occured");
            }
        } catch (Exception e) {
            // Could not send a message to the member
            logger.severe(e.toString());
        }
        return false;
    }
}
