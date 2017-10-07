package edu.illinois.cs.cs425;

import java.net.*;
import java.util.*;
import java.io.*;
import org.json.JSONArray;
import org.json.JSONObject;
import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This class is to handle the message received by listener
 */
public class MessageHandler extends Thread{
      private final static Logger logger = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);
	DatagramPacket receivePacket;
      DatagramSocket server;
      CopyOnWriteArrayList<String> members;
      CopyOnWriteArrayList<String> master_list;
      CopyOnWriteArrayList<String> files;
      boolean leader;
      boolean master_flag = false;
      String timestamp;
      int portnumber;
      int sdfsPort;
      private Random rand;

	public MessageHandler(DatagramSocket _server, DatagramPacket _receivePacket, CopyOnWriteArrayList<String> _members, CopyOnWriteArrayList<String> _master_list,CopyOnWriteArrayList<String> _files,boolean _leader, String _timestamp, int _portnumber,int _sdfsPort) { 
            // TODO check that later
		server = _server;
            receivePacket = _receivePacket;
            members = _members;
            master_list = _master_list;
            files=_files;
            leader = _leader;
            timestamp = _timestamp;
            portnumber = _portnumber;
            sdfsPort = _sdfsPort;
            rand = new Random();
	}

      /** 
       * This function is to print current member list to screen
       */
      public void printMemberlist()
      {
        try{
            logger.finest("Current member list is:");
            Iterator<String> it = members.iterator();
            System.out.println("*******************************");
            int i=0;
            String selfID = InetAddress.getLocalHost().getHostName()+"//"+Integer.toString(portnumber)+"//"+timestamp+"//"+Integer.toString(sdfsPort);
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
       * This function is to broadcast message to all member in the group
      **/
      public void broadCast(String keyWord,String content)
      {
            try{
                  Iterator<String> it = members.iterator();
                  JSONObject jsonBroadcast = new JSONObject();
                  jsonBroadcast.put(keyWord,content);
                  if(master_flag==true){
                    jsonBroadcast.put("master","true");
                  }
                  while (it.hasNext()) {      
                        String member = it.next();
                        byte[] sendBroadcast = jsonBroadcast.toString().getBytes();
                        InetAddress memberIP = InetAddress.getByName(member.split("//")[0]);
                        String memberPort = member.split("//")[1];
                        DatagramPacket broadcastPacket = new DatagramPacket(sendBroadcast,sendBroadcast.length,memberIP,Integer.parseInt(memberPort));
                        
                        if (Daemon.simulate) {
                            int randNum = rand.nextInt(100);
                            if (randNum <= Daemon.rate) {
                                logger.severe("Dropping packet");
                            } else {
                                server.send(broadcastPacket); // Ping is sent
                            }
                        } else {
                            server.send(broadcastPacket); // Ping is sent
                        }

                        logger.finest(keyWord+" message broadcast to "+member);
                  }
            }catch(Exception e)
            {
                  logger.finest(e.toString());
            }
            
      }

      /** This function is to distinguish message type and take different reactions
       * Message type:
       * -join: leader send back the current memberlist, broadcast in the group and add the member to list; common member add member to list
       * -init: this perticular message is sent by leader only. follow by a array of member list.
       * -ping: send ack message back to the source
       * -pingreq: send ping message to target
       * -ack: this perticular message appears in indirect ping process. once receive the "ack" message, the process send another "ack" message to the source who start the pingreq.
       * -leave/-fail: remove the target from the member list 
      */
	public void run() {
		try {
			logger.finest("MessageHandler started");

                  // Declare a new UDP packet for ack back 
                  DatagramPacket ackPacket;
                  JSONObject jsonAck = new JSONObject();
                  byte[] sendAck;
                  String ackMessage;
      

                  // Use JsonObject to handle the receiving message
                  String message = new String(receivePacket.getData());
                  JSONObject jsonArgument = new JSONObject(message);

                  if (jsonArgument.has("join")) { 
                        //If I am a leader, then broadcast , send the member list back | add new member to memberlist             	
                        String target = jsonArgument.getString("join");
                        logger.finest("New join member"+target);
                        
                        if (leader) {
                              // Put all members in list to a JSONObject. Including leader itself
                              if(jsonArgument.has("master")){
                                master_flag = true;
                                logger.info("This is a master");
                              }
                              if(master_flag==true){
                                master_list.add(target);
                              }    
                              JSONArray memberlst_array = new JSONArray();
                              for (String member : members) {
                                    memberlst_array.put(member);
                              }
                              // Don't forget to add leaderID to the list 
                              String leaderID = InetAddress.getLocalHost().getHostName()+"//"+Integer.toString(portnumber)+"//"+timestamp+"//"+Integer.toString(sdfsPort);
                              memberlst_array.put(leaderID);
                              logger.finest("add leaderID "+leaderID);
                              jsonAck.put("init",memberlst_array);
                              logger.finest("Member list prepared!");
                              JSONArray masterlst_array = new JSONArray();
                              for(String master_member: master_list){
                                masterlst_array.put(master_member);
                              }
                              jsonAck.put("init_m",masterlst_array);
                              logger.finest("Master list prepared");

                              if(master_flag==true){
                                JSONArray fileLst_array = new JSONArray();
                                logger.info("About to put files");
                                for(String file: files){
                                  logger.info("Putting file " + file);
                                  fileLst_array.put(file);
                                }
                                jsonAck.put("init_f",fileLst_array);
                                logger.info("File list prepared");
                              }

                              // Send memberlist back to the new join member
                              String memberlstInit = jsonAck.toString();
                              sendAck= memberlstInit.getBytes();
                              InetAddress targetIP = InetAddress.getByName(target.split("//")[0]);
                              String targetPort = target.split("//")[1];
                              ackPacket = new DatagramPacket(sendAck,sendAck.length,targetIP,Integer.parseInt(targetPort));
                              server.send(ackPacket); // Ping is sent
                              // if (Daemon.simulate) {
                              //     int randNum = rand.nextInt(100);
                              //     if (randNum <= Daemon.rate) {
                              //         logger.severe("Dropping packet");
                              //     } else {
                              //         server.send(ackPacket); // Ping is sent
                              //     }
                              // } else {
                              //     server.send(ackPacket); // Ping is sent
                              // }

                              logger.finest("send initial memberlist to "+targetIP.toString()+":"+targetPort);

                              //TODO add master information in the join procedure  
                                                  
                              // Broadcast the join message to all the members
                              for (int i = 0; i < 3; i++) {
                                    logger.finest("Broadcast round: "+Integer.toString(i));
                                    broadCast("join",target);
                                    Thread.sleep(100);  
                              }

                              // Finally add the new join member to member list
                              members.add(target);
                              //
                              printMemberlist();
                        }
                        else {
                              //for common process(not the leader), check if the remoteID already in the memberlist, 
                              if(jsonArgument.has("master")){
                                if(!master_list.contains(target)){
                                  master_list.add(target);
                                  logger.finest("add new member to master list!");
                                }else{
                                  logger.finest("member already exist in master_list");
                                }
                              }else{
                                logger.finest("The new join member is not master");
                              }
                              if (!members.contains(target)) {
                                    members.add(target);
                                    logger.finest("add new member success!");
                                    printMemberlist();
                              }
                              else{
                                    logger.finest("member already exist");
                              }
                              
                              
                        }
                  } else if (jsonArgument.has("ping")) {
                  	logger.finest(jsonArgument.getString("ping"));
                        String seqNum = jsonArgument.getString("ping");
                        String srcIp = jsonArgument.getString("srcIp");
                        String srcPort = jsonArgument.getString("srcPort");
                        String srcID = jsonArgument.getString("id");
                        logger.finest("receive ping from "+srcIp+"//"+srcPort+"//"+srcID);
                        logger.finest("seqNum of this ping is "+seqNum);

                        // Checking if this is a relay ping (For pingReq)
                        if (jsonArgument.has("isPingReq") && jsonArgument.has("returnPort") && jsonArgument.has("returnIp")) {
                              String returnIP = jsonArgument.getString("returnIp");
                              String returnPort = jsonArgument.getString("returnPort");
                              jsonAck.put("returnIp", returnIP);
                              jsonAck.put("returnPort", returnPort);
                              logger.severe("Got relay ping for " + srcID);
                        }

                        //Check if it is the right number
                        if (srcID.equals(timestamp)) {
                              logger.finest("preparing ack for "+seqNum);
                              jsonAck.put("ack",seqNum);
                              ackMessage = jsonAck.toString();
                              sendAck = ackMessage.getBytes();
                              ackPacket = new DatagramPacket(sendAck,sendAck.length,InetAddress.getByName(srcIp),Integer.parseInt(srcPort));
                              
                              if (Daemon.simulate) {
                                  int randNum = rand.nextInt(100);
                                  if (randNum <= Daemon.rate) {
                                      logger.severe("Dropping packet");
                                  } else {
                                      server.send(ackPacket); // Ping is sent
                                  }
                              } else {
                                  server.send(ackPacket); // Ping is sent
                              }

                              logger.finest("ack back sent for " + seqNum);
                              logger.finest("ack packet size is " + sendAck.length);
                        } else {
                          logger.severe("Got a ping but for not my ID");
                        }
                  } else if (jsonArgument.has("init")) {
                        logger.finest("receiving initial member list from leader!");
                  	JSONArray membership_array = jsonArgument.getJSONArray("init");
                  	
                        for (int i = 0; i < membership_array.length(); i++) {
                  		String member = membership_array.getString(i);
                  		members.add(member);
                              logger.finest("Adding member " + member);
                  	} 
                    if(jsonArgument.has("init_m")){
                      JSONArray masterlst_array = jsonArgument.getJSONArray("init_m");
                      logger.finest("receiving initial master list from leader");
                      for(int i =0;i<masterlst_array.length();i++){
                        String master = masterlst_array.getString(i);
                        master_list.add(master);
                        logger.finest("Adding master "+master);
                      }
                      if(jsonArgument.has("init_f")){
                        JSONArray file_list = jsonArgument.getJSONArray("init_f");
                        logger.info("receiving inital file list from master");
                        for(int i=0;i<file_list.length();i++){
                          String fileName = file_list.getString(i);
                          files.add(fileName);
                        }
                        logger.info("finish setting up file list");
                      }
                    } 
                    
                        printMemberlist();  	
                  } else if (jsonArgument.has("pingReq")) {
                        logger.finest("Got a pingReq message");
                        String seqNum = jsonArgument.getString("pingReq");
                        String target = jsonArgument.getString("target");
                        String targetIP = target.split("//")[0];
                        String targetPort = target.split("//")[1];
                        String targetID = jsonArgument.getString("id");
                        logger.finest("ID got here is " + targetID);
                        String returnIP = jsonArgument.getString("srcIp");
                        String returnPort = jsonArgument.getString("srcPort");
                        
                        InetAddress IPAddress = InetAddress.getByName(targetIP);
                        byte[] sendData;

                        JSONObject pingObj = new JSONObject();
                        pingObj.put("ping", seqNum);
                        pingObj.put("id", targetID); // This is the unique ID of the node we are pinging
                        pingObj.put("srcIp", InetAddress.getLocalHost().getHostAddress());
                        pingObj.put("srcPort", Integer.toString(portnumber));
                        pingObj.put("isPingReq", "true");
                        pingObj.put("returnIp", returnIP);
                        pingObj.put("returnPort", returnPort);
                        
                        String ping = pingObj.toString();
                        sendData = ping.getBytes();

                        logger.finest("Connecting to " + IPAddress.toString() + " via UDP port " + targetPort);
                        logger.finest("Sending " + sendData.length + " bytes to server");
                        logger.finest("Sending packet = " + ping);
                        
                        DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, IPAddress, Integer.parseInt(targetPort)); 
                        
                        if (Daemon.simulate) {
                            int randNum = rand.nextInt(100);
                            if (randNum <= Daemon.rate) {
                                logger.severe("Dropping packet");
                            } else {
                                server.send(sendPacket); // Ping is sent
                            }
                        } else {
                            server.send(sendPacket); // Ping is sent
                        }

                        logger.finest("Sent " + sendData.length + " bytes to server");
                  } else if (jsonArgument.has("ack")) {
                        // Relaying back the ACK
                        String seqNum = jsonArgument.getString("ack");
                        String returnIP = jsonArgument.getString("returnIp");
                        String returnPort = jsonArgument.getString("returnPort");
                        
                        InetAddress IPAddress = InetAddress.getByName(returnIP);
                        byte[] sendData;

                        JSONObject pingObj = new JSONObject();
                        pingObj.put("ack", seqNum);
                        pingObj.put("srcIp", InetAddress.getLocalHost().getHostAddress());
                        pingObj.put("srcPort", Integer.toString(portnumber));
                        
                        String ping = pingObj.toString();
                        sendData = ping.getBytes();

                        logger.finest("Connecting to " + IPAddress.toString() + " via UDP port " + returnPort);
                        logger.finest("Sending " + sendData.length + " bytes to server");
                        logger.finest("Sending packet = " + ping);
                        
                        DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, IPAddress, Integer.parseInt(returnPort)); 
                        
                        if (Daemon.simulate) {
                            int randNum = rand.nextInt(100);
                            if (randNum <= Daemon.rate) {
                                logger.severe("Dropping packet");
                            } else {
                                server.send(sendPacket); // Ping is sent
                            }
                        } else {
                            server.send(sendPacket); // Ping is sent
                        }

                        logger.finest("Sent " + sendData.length + " bytes to server");
                  } else if (jsonArgument.has("leave") || jsonArgument.has("fail")) {
                        if (jsonArgument.has("leave")) {
                              logger.finest(jsonArgument.getString("leave"));
                              String target = jsonArgument.getString("leave");
                              logger.finest("Leave claim at member "+target);
                              if(master_list.contains(target)){
                                master_list.remove(target);
                              }else{
                                logger.finest("Member "+ target +" is not in the master list");
                              }
                              if(members.contains(target)){
                                    boolean status = members.remove(target);
                                    logger.finest("Remove member "+target+" status "+status);
                                    printMemberlist();
                              }
                              else{
                                    logger.finest("Cannot remove "+ target+"\n"+"Member "+target+" already removed");
                              }
                              
                              
                        } else if (jsonArgument.has("fail")) {
                              String target = jsonArgument.getString("fail");
                              String srcIp = jsonArgument.getString("srcIp");
                              String srcPort = jsonArgument.getString("srcPort");
                              logger.finest("Receive failure detection message from " +srcIp+":"+srcPort);
                              if(master_list.contains(target)){
                                master_list.remove(target);
                              }else{
                                logger.finest("Member "+ target +" is not in the master list");
                              }
                              if(members.contains(target))
                              {
                                    boolean status = members.remove(target);
                                    logger.finest("Remove member "+target+" status "+status);
                                    printMemberlist();
                                    /*logger.info("Request file list clear");
                                    String selfIP = InetAddress.getLocalHost().getHostName();
                                    int selfPort = sdfsPort;
                                    Socket sdfsClient = new Socket(selfIP,selfPort);
                                    DataOutputStream dos = new DataOutputStream(sdfsClient.getOutputStream());
                                    dos.writeUTF("clear "+target);
                                    dos.flush();
                                    sdfsClient.close();
                                    dos.close();
                                    logger.info("Request send");*/
                              }

                        }
                        //TODO delete from the memberlist;
                  }
		} catch(Exception e){
			logger.severe(e.toString());
		}
	}
}

