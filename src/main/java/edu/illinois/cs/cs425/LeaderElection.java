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

public class LeaderElection extends Thread{
	private final static Logger logger = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);
	CopyOnWriteArrayList<String> master_list;
	CopyOnWriteArrayList<String> members;
	CopyOnWriteArrayList<String> files;
	

	public LeaderElection (CopyOnWriteArrayList<String> _master_list,CopyOnWriteArrayList<String> _members ,CopyOnWriteArrayList<String> _files){
		master_list = _master_list;	
		members = _members;
		files = _files;
	}
	private void broadCast(String message){
	logger.info("New master has been elected!"+'\n'+" Start broadcasting the good news!");
		for(String member : members){
			//broadcast to all members except masters
			Socket client = null;
			DataOutputStream dos = null;	
			try{
				String member_Ip = member.split("//")[0];
				int member_Port = Integer.parseInt(member.split("//")[3]);
				client = new Socket(member_Ip,member_Port);
				// PrintWriter pw = new PrintWriter(client.getOutputStream(),true);	
				// pw.println(message);
				dos = new DataOutputStream(client.getOutputStream());
				dos.writeUTF(message);
				dos.flush();
				logger.info("New master broadcast send to: "+member);
			}catch(Exception e){
				logger.severe(e.toString());
			} finally {
				try{
					client.close();
					dos.close();
					}catch(Exception e){
					logger.severe(e.toString());
					}			
			}
		
		}
	}
	public void run(){
		try{	
			Socket client;
			int failureNumber = 3-master_list.size();;
			logger.info("Find master failure: "+Integer.toString(failureNumber));
			String max[]= new String[2];
			String candidate[] = new String[2];
			max[0]=max[1]=" ";
			for (String member : members) {
				if(!master_list.contains(member)){
					String id = member.split("//")[2];
					if(id.compareTo(max[0])>0){
						max[1]=max[0];
						candidate[1] = candidate[0];
						max[0] = id;
						candidate[0]=member;
					}else if(id.compareTo(max[1])>0){
						max[1] = id;
						candidate[1]=member;
					}
				}else{
					logger.info(member+" is already in master list");
				}
				
			}
			for(int i=0;i<failureNumber;i++){
				// TODO send out file scp?
				logger.info("Select new master candidate: " +candidate[i]);
				String master_Ip = candidate[i].split("//")[0];
				int master_Port = Integer.parseInt(candidate[i].split("//")[3]);
				client = new Socket(master_Ip,master_Port);
				// PrintWriter output = new PrintWriter(client.getOutputStream(), true);
				// output.println("master "+candidate[i]);
				DataOutputStream dos = new DataOutputStream(client.getOutputStream());
				dos.writeUTF("master " + candidate[i]);
				dos.flush();
				logger.info("Inform candidate!");
				// TODO change this
				
	            
	            logger.info("Starting sending log to candidate: "+master_Ip+"//"+Integer.toString(master_Port));
	            String filesToSend = "";
	            for(String fileContent:files) {
	            	logger.info(fileContent);
	                filesToSend += fileContent;
	                filesToSend += " ";
	            }
	            if (filesToSend.length() != 0) {
	            	filesToSend = filesToSend.substring(0, filesToSend.length()-1);	
	            }
	            dos.writeUTF(filesToSend);
	            dos.flush();
				logger.info("Finish sending log file to candidate: "+candidate[i]);

				master_list.add(candidate[i]);
				broadCast("master "+candidate[i]);
				client.close();
				dos.close();
			}	
		}catch(Exception e){
			logger.info(e.toString());
		}
}
}
