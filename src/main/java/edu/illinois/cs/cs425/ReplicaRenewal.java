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

public class ReplicaRenewal extends Thread{
	private final static Logger logger = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);
	CopyOnWriteArrayList<String> members;
	CopyOnWriteArrayList<String> master_list;
    CopyOnWriteArrayList<String> files;
    CopyOnWriteArrayList<String> renewalList;
    DataOutputStream output;
    DataInputStream input;
    Socket client;
    boolean sucess;

    String failMember;

    public ReplicaRenewal(CopyOnWriteArrayList<String> _members, CopyOnWriteArrayList<String> _master_list,String _failMember,CopyOnWriteArrayList<String> _files){
		members = _members;
		failMember = _failMember;
		master_list = _master_list;
		files = _files;
		renewalList = new CopyOnWriteArrayList<String>();
		sucess = false;
	}
	/**
	 * This function is to check whether a member is alive
	 */
	
	/**
	 * This function is to find a live owner of a file and renew it
	 **/
	public void renew(String sdfsFile){
		try{		
			for(String file:files){
				if(sdfsFile.equals(file.split("//")[0])){
					String owner = file.split("//")[1]+"//"+file.split("//")[2];

					//client.setSoTimeout(15000);
					logger.info("Find one owner of file: "+sdfsFile+", check if it is alive "+owner);
					try{
						logger.info("Contack with file owner "+owner+": "+sdfsFile);
						client = new Socket(owner.split("//")[0], Integer.parseInt(owner.split("//")[1]));
					}catch(Exception e){
						logger.info("Try owner "+owner+". Failed");
						continue;
					}		
					String renewPlace = "#";
					logger.info("Start find new place for replica");
					for(String member:members){
						if(!files.contains(sdfsFile+"//"+member.split("//")[0]+"//"+member.split("//")[3]))
						{
							renewPlace = member.split("//")[0]+"//"+member.split("//")[3];
							logger.info("Find new place for replica! "+ renewPlace);
							files.add(sdfsFile+"//"+renewPlace);
							break;
						}
					}
					if(renewPlace.equals("#")){
						String selfID = InetAddress.getLocalHost().getHostName()+"//"+Integer.toString(Daemon.sdfsPort);
						if(!files.contains(sdfsFile+"//"+selfID)){
							renewPlace = selfID;
							logger.info("Find new place for replica! "+ renewPlace);
							files.add(sdfsFile+"//"+renewPlace);
						}else{
							logger.severe("cannot find another place for file: "+sdfsFile);
						}
					}
					if(renewPlace.equals("#")){
						logger.info("Skip renewal process. cannot find place for renewal");
					}else{
						try{
							logger.info("Prepared for renew massage");
							input = new DataInputStream(client.getInputStream());
							output = new DataOutputStream(client.getOutputStream());
							output.writeUTF("renew " + sdfsFile+" "+renewPlace);
		            		output.flush();
		            		logger.info("Send renew message, wait for reply");
		            		String reply = input.readUTF();
		            		if(reply.equals("sucess")){
		            			logger.info("renew replica sucess");
		            			sucess = true;
		            			//files.add(sdfsFile+"//"+renewPlace);
		            			client.close();
								input.close();
								output.close();
		            			break;
		            		}else{
		            			logger.info("Owner does not reply, try another Owner");
		            			files.remove(sdfsFile+"//"+renewPlace);
		            			logger.info("Renewal failed"+"\n"+"Remove "+ sdfsFile+"//"+renewPlace);
		            			continue;
		            		}
						}catch(Exception e){
							logger.severe(e.toString());
							files.remove(sdfsFile+"//"+renewPlace);
							logger.info("Renewal failed"+"\n"+"Remove "+ sdfsFile+"//"+renewPlace);
							continue;

						}
						
					}
					
				}
			}
		}catch(Exception e){
				logger.info(e.toString());
			}
					
				
		}
	
		
	
	/**
	  * This function is to find how which files the failed node had and find new replica
	  **/
	public void run(){
		try{
			logger.info("ReplicaRenewal running");
			logger.info(failMember+" is down. Files needs renewing");
			
			String nameOfFail = failMember.split("//")[0]+"//"+failMember.split("//")[3];
			for(String file:files){
				String name = file.split("//")[1]+"//"+file.split("//")[2];
				if(name.equals(nameOfFail)){
					renewalList.add(file.split("//")[0]);
					logger.info("node "+nameOfFail+" has "+file.split("//")[0]+", need renewal");
				}
			}
			for(String sdfsFile:renewalList){
				if(files.contains(sdfsFile+"//"+failMember.split("//")[0]+"//"+failMember.split("//")[3])){
					files.remove(sdfsFile+"//"+failMember.split("//")[0]+"//"+failMember.split("//")[3]);
					logger.info("Remove "+sdfsFile+"//"+failMember.split("//")[0]+"//"+failMember.split("//")[3]+" from file list");
					logger.info("Start renewing "+sdfsFile);
					renew(sdfsFile);
					logger.info("renewed "+sdfsFile);
					logger.info("Start informing other master");
					String self = InetAddress.getLocalHost().getHostName()+"//"+Integer.toString(Daemon.listenerPort)+"//"+Daemon.timeStamp+"//"+Integer.toString(Daemon.sdfsPort);
                	for(String dstMaster: master_list){
                    	if(dstMaster.equals(self)){
                    		logger.info("Skip self");
                    	}else{
                    		String dstIP = dstMaster.split("//")[0];
                    		int dstPort = Integer.parseInt(dstMaster.split("//")[3]);
                    		client = new Socket(dstIP,dstPort);
                    		output  = new DataOutputStream(client.getOutputStream());
                    		output.writeUTF("clear "+failMember);
                    		output.flush();
                    	}
                	}
                	logger.info("Finish informing other master");
				}			
			}
			logger.info("ReplicaRenewal exit");
		}catch(Exception e){
			logger.info(e.toString());
		}
	}

}