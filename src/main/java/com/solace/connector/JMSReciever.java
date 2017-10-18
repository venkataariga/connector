package com.solace.connector;

import java.util.Hashtable;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination; 
import javax.jms.ExceptionListener;
import javax.jms.JMSException; 
import javax.jms.Message;
import javax.jms.MessageConsumer; 
import javax.jms.MessageListener;
import javax.jms.Session;
import javax.naming.Context;
import javax.naming.InitialContext;
import com.solacesystems.jms.SupportedProperty;
import org.apache.log4j.Logger;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;

public class JMSReciever  extends Receiver<String> implements MessageListener{


	
	private static final Logger log = Logger.getLogger(JMSReciever.class); 



	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	
	private static final String SOLJMS_INITIAL_CONTEXT_FACTORY = "com.solacesystems.jndi.SolJNDIInitialContextFactory";
	
	private StorageLevel storageLevel;
	private String brokerURL; 
	private String vpn;
	private String username;
	private String password;
	private String queueName; 
	private String connectionFactory; 
	private Connection connection;

	
	
	public JMSReciever(String brokerURL, String vpn, String username, String password, String queueName,String connectionFactory, StorageLevel storageLevel)
	{
	super(storageLevel);
	this.storageLevel = storageLevel; 
	this.brokerURL = brokerURL;
	this.vpn = vpn;
	this.username = username;
	this.password = password;
	this.queueName = queueName; 
	this.connectionFactory = connectionFactory;
	}
	
	public void onStart() {
		log.info("Starting up...");
		try{
			Hashtable<String, String> env = new Hashtable<String, String>();
			env.put(InitialContext.INITIAL_CONTEXT_FACTORY,
		SOLJMS_INITIAL_CONTEXT_FACTORY);
			
		env.put(InitialContext.PROVIDER_URL, brokerURL);
		env.put(Context.SECURITY_PRINCIPAL, username);
		env.put(Context.SECURITY_CREDENTIALS, password);
		env.put(SupportedProperty.SOLACE_JMS_VPN, vpn);
		
		javax.naming.Context context = new javax.naming.InitialContext(env);
		
		ConnectionFactory factory = (ConnectionFactory)context.lookup(connectionFactory);
	    Destination queue = (Destination) context.lookup(queueName);
		connection = factory.createConnection(); 
//		_connection.setExceptionListener(new JMSReceiverExceptionListener());
		Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);

		MessageConsumer consumer;
		consumer = session.createConsumer(queue); consumer.setMessageListener(this);
	     connection.start();
		log.info("Completed startup.");
		} 
		catch (Exception ex)
		{
		    // Caught exception, try a restart
		log.error("Callback onStart caught exception, restarting ", ex);
		restart("Callback onStart caught exception, restarting ", ex);
		    }
		}
	
	public void onStop() {
		log.info("Callback onStop called"); try
		{
		connection.close(); 
		} catch (JMSException ex) {
		log.error("onStop exception", ex);
		}
		}
	
	@Override
	public void onMessage(Message message) {
	log.info("Callback onMessage received" + message);
	store(message.toString());
	try {
	message.acknowledge(); 
	} catch (JMSException ex) 
	{
	log.error("Callback onMessage failed to ack message", ex);
	}
	}




}
