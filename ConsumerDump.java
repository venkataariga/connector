package com.solace.cassandra;
//import com.datastax.driver.core.BoundStatement;
//import com.datastax.driver.core.PreparedStatement;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Calendar;
import java.util.Date;
import java.util.Properties;
import java.sql.*;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

import com.solacesystems.jcsmp.BytesXMLMessage;

import com.solacesystems.jcsmp.FlowReceiver;
import com.solacesystems.jcsmp.InvalidPropertiesException;
import com.solacesystems.jcsmp.JCSMPChannelProperties;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.JCSMPTransportException;
import com.solacesystems.jcsmp.Queue;
import com.solacesystems.jcsmp.TextMessage;
import com.solacesystems.jcsmp.XMLMessageListener;

public class ConsumerDump implements XMLMessageListener {

	Logger logger = Logger.getLogger(ConsumerDump.class);

	String msgtype="";
	Properties solProps = null;

	DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
	
	String query = " insert into updev (Solace_date_time,IMEI_DI1,Device_date_time_DI5,PLMN_LI1,Firmware_version_DI5,Message_type_ne_de_ae_nc_dc_ac_,Event_Name_name,messageId)"+" values (?,?,?,?,?,?,?,?)";
	java.sql.PreparedStatement preparedStmt ;

	JCSMPSession session = null;

	File messagesFile = null;
	private Connection con;
	// XMLMessageListener
	public void onException(JCSMPException exception) {
		logger.error("Exception occured with XMLMessageListener " + exception);
		System.out.println("Exception occured with XMLMessageListener " + exception);
	}

	String rxdata = "";


	public void onReceive(BytesXMLMessage msg) {
		try {
			
			if (msg instanceof TextMessage) {
				rxdata = ((TextMessage) msg).getText();
			} else {
				byte[] messageBinaryPayload = msg.getAttachmentByteBuffer().array();
				rxdata = new String(messageBinaryPayload, "UTF-8");
			}
			logger.info("Data from Solace Box : " + rxdata);
			String msgid=msg.getMessageId();
			
//			if(solProps!=null){
//				String dirPath = solProps.getProperty("file.dir");
//		  	messagesFile = new File(dirPath + df.format(new Date()) + ".csv");
//				messagesFile = new File(df.format(new Date()) + ".csv");
//			}
//			if (!messagesFile.exists()) {
//				messagesFile.createNewFile();
////				writer = new FileWriter(messagesFile);
//			}
			JSONArray array = null;
			try {
				array = new JSONArray(rxdata);
			} catch (Exception e) {
				logger.error(
						"******Message Failed due to CONVERSION rx datamsg to JSON Array :*******  and the data is  "
								+ rxdata);
//			         msg.ackMessage();
				throw e;
			}
			String IMEI = "";
			logger.debug("Message Evt size : " + array.length());
			
			for (int i = 0; i < array.length(); i++) {

				JSONObject jsonObj = array.getJSONObject(i);
				
				String DATE="";
				String plmn="";
				String firmVersion = "";
//				String msgid=msg.getMessageId();
				String name="";
				try {	IMEI = jsonObj.getString("DI1");}
				catch(Exception e) {
					IMEI = "NOT FOUND";
				}
				try {		DATE = jsonObj.getString("DT");}
				catch(Exception e) {
					DATE = "NOT FOUND";
				}

				try { 		firmVersion = jsonObj.getString("DI5");}
				catch(Exception e) {
					firmVersion = "NOT FOUND";
				}
				try{		name =jsonObj.getString("Name");}
				catch(Exception e) {
					name = "NOT FOUND";
				}
				try {		plmn = jsonObj.getString("LI1");}
				catch(Exception e) {
					System.err.print("******Following not found in jason object************"+e.getMessage());
					plmn = "NOT FOUND";
				}		
				    
				Timestamp sq = new Timestamp(System.currentTimeMillis());
//				String query = " insert into dev (IMEI,DATE,plmn,firmVersion,timestamp)"+" values (?, ?, ?, ?, ?)";
//				java.sql.PreparedStatement preparedStmt = con.prepareStatement(query);
				String curdate=sq.toString();
				preparedStmt.setString(1, curdate);
			      preparedStmt.setString (2, IMEI);
			      preparedStmt.setString (3, DATE);
			      preparedStmt.setString (4, plmn);
			      preparedStmt.setString(5, firmVersion);
			      preparedStmt.setString(6, msgtype);
			      preparedStmt.setString(7, name);
			      preparedStmt.setString(8, msgid);
			      

			      // execute the preparedstatement
			      preparedStmt.addBatch();
			      
			      

//				PreparedStatement prepared = client.getSession().prepare("insert into "+solProps.getProperty("table.name")+""
//						+ " (di1,di3,dt,message) values (?,?,?,?)");
//
//				BoundStatement bound = prepared.bind(Long.valueOf(IMEI), Long.valueOf(DATE),Long.valueOf(plmn),Long.valueOf(firmVersion),Long.valueOf(sq.toString()));
//				logger.info(IMEI + "            " + DATE + "            " + plmn + "            " + firmVersion+"          "+sq.toString());
//				writer.append(IMEI);
//				writer.append(',');
//				writer.append(DATE);
//				writer.append(',');
//				writer.append(plmn);
//				writer.append(',');
//				writer.append(firmVersion);
//				writer.append(',');
//				writer.append(sq.toString());
//				writer.append('\n');
				

			}
			preparedStmt.executeBatch();
//			 msg.ackMessage();

		} catch (UnsupportedEncodingException e) {
//			 msg.ackMessage();
			System.err.println("******Message Failed while1111111111 operating the parser:*******" + " and the data is  " + rxdata
					+ " stack Trace:" + e.getMessage());
		}

		catch (Exception ex) {
//			 msg.ackMessage();
			System.err.println("******Message Failed while22222222222 operating the parser:*******" + " and the data is  " + rxdata
					+ " stack Trace:" + ex.getMessage());
		} 
//		finally {
//			System.out.println("***** Sql insertion Try Block***");
////			try {
////				if (writer != null) {
////					writer.flush();
////					 writer.close();
////				}
////			} catch (IOException e) {
////				logger.error("Exception occured due to wiritng the file content on CSV");
////			}
//		}
	}

	void createSession(String[] args) throws InvalidPropertiesException {
//		java.lang.Class.forName("com.mysql.jdbc.Driver");  
//	    "jdbc:mysql://localhost:3306/dc","root","root"
	    
		System.out.println("Initialzing the Solace & Mysql Connections");
//		ADD---->2	      
//	   	   Properties solProps = ResourceLoader.getSolaceProperties();
//			 String uname = solProps.getProperty("mysql.uname");
//			 String passwd = solProps.getProperty("mysql.passwd");
//			 String url = solProps.getProperty("mysql.url");
//			  System.out.println("Loaded these"+uname+passwd+url);
//			 try {
//			 client.connect(uname,passwd,url);
//			 System.out.print("Connected succesfully to"+uname+"\t"+passwd+"\t"+url);
//			 }
//			 catch (Exception e) {System.out.println(e.toString());}
////			  System.out.println("Cassandra Connection Established");
//////		ADD---->2
//			 try{  
//				    java.lang.Class.forName("com.mysql.jdbc.Driver");  
////				    "jdbc:mysql://localhost:3306/dc","root","root"
//				    con=DriverManager.getConnection(url,uname,passwd);
//				    System.out.println("Connected to mysql db!!");
//				      
//				    }
//	    		catch(Exception e){ System.out.println(e);}  
				     
		logger.info("Initialzing the Solace Connections");
	
		try {
			System.out.println("Loading solprops");
			solProps = ResourceLoader.getSolaceProperties();
			System.out.println("Loaded solprops");
			final JCSMPProperties properties = new JCSMPProperties();
 
			String uname = solProps.getProperty("mysql.uname");
			 String passwd = solProps.getProperty("mysql.passwd");
			 String url = solProps.getProperty("mysql.url");
			 msgtype=solProps.getProperty("mysql.messagetype");
			  System.out.println("Loaded these"+uname+passwd+url);
			  try{  
				    Class.forName("com.mysql.cj.jdbc.Driver");  
				    con=DriverManager.getConnection(url,uname,passwd);
				    System.out.println("Connected to mysql db!!");
				    preparedStmt = con.prepareStatement(query);
				      
				    }
	    		catch(Exception e){ System.out.println(e.toString());}  
			
			properties.setProperty(JCSMPProperties.HOST, solProps.getProperty("solace.host"));
			properties.setProperty(JCSMPProperties.VPN_NAME, solProps.getProperty("solace.vpn"));
			properties.setProperty(JCSMPProperties.USERNAME, solProps.getProperty("solace.username"));
			properties.setProperty(JCSMPProperties.PASSWORD, solProps.getProperty("solace.password"));
			properties.setProperty(JCSMPProperties.MESSAGE_ACK_MODE, JCSMPProperties.SUPPORTED_MESSAGE_ACK_CLIENT);
			JCSMPChannelProperties cp = (JCSMPChannelProperties) properties
					.getProperty(JCSMPProperties.CLIENT_CHANNEL_PROPERTIES);

			session = JCSMPFactory.onlyInstance().createSession(properties);

			logger.info("Solace Connection Established");
            String dirPath = solProps.getProperty("file.dir");
			messagesFile = new File(dirPath + df.format(new Date()) + ".csv");
//			writer = new FileWriter(messagesFile);
		} catch (Exception e) {
			logger.error("Unable to load the Solace Properties");
		}

	}

	public void run(String[] args) {
		FlowReceiver receiver = null;
		boolean blnRunFlag = true;
		try {
			// Create the Session.
			createSession(args);
			Properties solProps = ResourceLoader.getSolaceProperties();
			Queue queue = JCSMPFactory.onlyInstance().createQueue(solProps.getProperty("queue.name"));
			// Create and start the receiver.
			receiver = session.createFlow(queue, null, this);
			while (blnRunFlag) {
				receiver.start();
				// Thread.sleep(10);
			}
			// Close the receiver.
			receiver.close();
		} catch (JCSMPTransportException ex) {
			logger.error("JCSMPTransportException occured while creating the flow : " + ex.getMessage());
			if (receiver != null) {
				receiver.close();
				blnRunFlag = false;
			}

		} catch (JCSMPException ex) {
			logger.error("JCSMPException occured while creating the flow : " + ex.getMessage());
			if (receiver != null) {
				receiver.close();
				blnRunFlag = false;
			}

		} catch (Exception ex) {
			logger.error("Exception occured while creating the flow : " + ex.getMessage());
			blnRunFlag = false;

		}
	}

}
