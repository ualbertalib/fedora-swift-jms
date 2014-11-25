package ca.ualberta.library.fedora.jms;

import static org.junit.Assert.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import javax.activation.DataHandler;
import javax.activation.DataSource;
import javax.activation.FileDataSource;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.TextMessage;
import javax.jms.ConnectionFactory;
import javax.jms.Connection;
import javax.jms.Session;
import javax.naming.Context;
import javax.xml.parsers.*;
import javax.xml.rpc.ServiceException;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpException;
import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.io.IOUtils;
import org.apache.activemq.*;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;
import org.fcrepo.client.FedoraClient;
import org.fcrepo.client.messaging.JmsMessagingClient;
import org.fcrepo.client.messaging.MessagingClient;
import org.fcrepo.client.messaging.MessagingListener;
import org.fcrepo.server.access.FedoraAPIAMTOM;
import org.fcrepo.server.errors.GeneralException;
import org.fcrepo.server.access.FedoraAPIA;
import org.fcrepo.server.errors.MessagingException;
import org.fcrepo.server.management.FedoraAPIMMTOM;
import org.fcrepo.server.messaging.JMSManager;
import org.fcrepo.server.types.gen.Datastream;
import org.fcrepo.server.types.gen.DatastreamControlGroup;
import org.fcrepo.server.types.gen.DatastreamDef;
import org.fcrepo.server.types.gen.RepositoryInfo;
import org.fcrepo.server.types.mtom.gen.MIMETypedStream;
import org.fcrepo.server.utilities.TypeUtility;
import org.fcrepo.common.Constants;
import org.jclouds.openstack.swift.domain.MutableObjectInfoWithMetadata;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import ca.ualberta.library.jclouds.JCloudSwift;

public class FedoraMessagingTest implements MessagingListener {

	private static Connection connection;
	private static Session session;
	private static Destination destination;
	
    MessagingClient messagingClient;
    private static FedoraAPIAMTOM APIA = null;
    private static FedoraAPIMMTOM APIM = null;
    private String swiftContainer = null;
    private String noidURL = null;
    private static String doc  = null;
    private HttpClient client;
	
    private class TopicListener implements MessageListener {
    	 
        /**
         * Casts the message to a TextMessage and displays its text.
         * A non-text message is interpreted as the end of the message 
         * stream, and the message listener sets its monitor state to all 
         * done processing messages.
         *
         * @param message  the incoming message
         */
        public void onMessage(Message message) {
            if (message instanceof TextMessage) {
                TextMessage  msg = (TextMessage) message;

//              testOnMessage("", message);
                
                try {
                    System.out.println("CONSUMER THREAD: Reading message: " 
                                       + msg.getText());
                } catch (JMSException e) {
                    System.out.println("Exception in onMessage(): " 
                                       + e.toString());
                }
            }
        }
    }
    
	@BeforeClass
	public static void setup() throws JMSException {
	    Document document = null;
        SAXReader reader = new SAXReader();
        
		ConnectionFactory connectionFactory = new ActiveMQConnectionFactory("vm://localhost?broker.persistent=false");
		connection = connectionFactory.createConnection();
		connection.start();
		session = connection.createSession(true, Session.SESSION_TRANSACTED);
		destination = session.createTopic("Test.Topic");
		
        try {
	        InputStream inputStream = new FileInputStream("src/test/files/message.xml");
	        document = reader.read(inputStream);
	        doc = document.asXML();
        }
        catch (DocumentException e) {
        	e.getMessage();
        }
        catch (FileNotFoundException e) {
        	e.getMessage();
        }
        
   }
	
	@AfterClass
	public static void cleanup() throws JMSException {
		session.close();
		connection.stop();
		connection.close();
	}
	
   @Test
   public void testStart() throws MessagingException {
    	
    	Properties jmsProperties = new Properties();
	  	try {
	  		jmsProperties.load(new FileInputStream("jms.properties"));
		  
		  	String factory = jmsProperties.getProperty("initialContextFactory");
		  	assertNotNull(factory);
		  	
		  	String url = jmsProperties.getProperty("providerURL");
		  	assertNotNull(url);
		  	
		  	String factoryName = jmsProperties.getProperty("connectionFactoryName");
		  	assertNotNull(factoryName);
		  	
		  	String messagingType = jmsProperties.getProperty("messagingType");
		  	assertNotNull(messagingType);
		  	
		  	String activityType = jmsProperties.getProperty("activityType");
		  	assertNotNull(activityType);
		  	
		  	String messagingClientName = jmsProperties.getProperty("messagingClientName");
		  	assertNotNull(messagingClientName);
	    	
	        Properties properties = new Properties();
	        properties.setProperty(Context.INITIAL_CONTEXT_FACTORY, factory);
	        assertNotNull(Context.INITIAL_CONTEXT_FACTORY);
	        
	        properties.setProperty(Context.PROVIDER_URL, url);
	        assertNotNull(Context.PROVIDER_URL);
	        
	        properties.setProperty(JMSManager.CONNECTION_FACTORY_NAME, factoryName);
	        assertNotNull(JMSManager.CONNECTION_FACTORY_NAME);
	        
	        properties.setProperty(messagingType, activityType);
	        assertNotNull(messagingType);
	        
	        messagingClient = new JmsMessagingClient(messagingClientName, this, properties, false);
	        messagingClient.start();
	        assertNotNull(messagingClient);

	  	}	  
	  	catch (IOException e) {
		  	e.printStackTrace();
	  	}
    	
    	Properties clientProperties = new Properties();
	  	try {
	  		clientProperties.load(new FileInputStream("client.properties"));
		  
		  	String baseURL = clientProperties.getProperty("baseURL");
	        assertNotNull(baseURL);
	        
		  	String username = clientProperties.getProperty("username");
	        assertNotNull(username);
	        
		  	String password = clientProperties.getProperty("password");
	        assertNotNull(password);
	        
			FedoraClient fedoraClient = new FedoraClient(baseURL, "fedoraAdmin", "fedoraAdmin");
			FedoraAPIA fedoraAPIA = fedoraClient.getAPIA();
			RepositoryInfo repositoryInfo = fedoraAPIA.describeRepository();
			String version = repositoryInfo.getRepositoryVersion();
			assertEquals(version, "3.7.0");
			
			APIA = fedoraClient.getAPIAMTOM();
			repositoryInfo = APIA.describeRepository();
			version = repositoryInfo.getRepositoryVersion();
			assertEquals(version, "3.7.0");
			
			APIM = fedoraClient.getAPIMMTOM();
			repositoryInfo = APIA.describeRepository();
			version = repositoryInfo.getRepositoryVersion();
			assertEquals(version, "3.7.0");
		}	  
	  	catch (IOException e) {
		  	e.printStackTrace();
	  	}
	  	catch (ServiceException e) {
		  	e.printStackTrace();
	  	}
	  	
    	Properties jcloudProperties = new Properties();
	  	try {
	  		jcloudProperties.load(new FileInputStream("swift.properties"));
		  
		  	swiftContainer = jcloudProperties.getProperty("container");
	        assertNotNull(swiftContainer);
	  	}	  
	  	catch (IOException e) {
		  	e.printStackTrace();
	  	}
    	
 /*   	Properties noidProperties = new Properties();
	  	try {
	  		noidProperties.load(new FileInputStream("noid.properties"));
		  
		  	noidURL = noidProperties.getProperty("url");
	        assertNotNull(noidURL);
		  	 
		  	client = new HttpClient();
	        assertNotNull(client);
		  	
	    	client.getHttpConnectionManager().getParams().setConnectionTimeout(50000);
	  	}	  
	  	catch (IOException e) {
		  	e.printStackTrace();
	  	}*/
   }
    
   @Test
   public void createMessageListener() throws JMSException {
	    MessageConsumer consumer = session.createConsumer(destination);
		TopicListener listener = new TopicListener();
		consumer.setMessageListener(listener);
		
		MessageProducer producer = session.createProducer(destination);
		Message message = session.createTextMessage(doc);
		producer.send(message);
		session.commit();
   }
	
   public void onMessage(String clientId, Message message) {
	   //dummy method		
   }
	
   @Test
   public void testOnMessage() throws JMSException {
	
	   Message message = session.createTextMessage(doc);
		
	   Document document = null;
       SAXReader reader = new SAXReader();
        
        String messageText = "";
        try {
            messageText = ((TextMessage)message).getText();
            
            InputStream inputStream = IOUtils.toInputStream(messageText, "UTF-8");
            document = reader.read(inputStream);
           
		    Element rootElement = document.getRootElement();

		    String titleText = null; 
	    	Iterator<Element> titleIterator = rootElement.elementIterator("title");
	    	while (titleIterator.hasNext()) {
	    		Element title = titleIterator.next();
	    		titleText = title.getText();
	    		assertEquals(titleText, "ingest");
	    	}
	    	
	    	String idText = null;
	    	if (titleText != null) {
	    		if (titleText.equals("ingest")) {
			    	Iterator<Element> idIterator = rootElement.elementIterator("summary");
			    	while (idIterator.hasNext()) {
			    		Element id = idIterator.next();
			    		idText = id.getText();
			    		assertEquals(idText, "changeme:1");
			    	}
	    		}
	    	}	
        } catch(JMSException e) {
        	e.getMessage();
	    } catch(IOException e) {
	    	e.getMessage();
	    } catch(DocumentException e) {
	    	e.getMessage();
	    }
	}

    @Test
 	public void testGetFedoraObject() {
		
    	String id = null;
    	
    	FedoraMessaging fedoraMessaging = new FedoraMessaging();
    	
		try {
        	InputStream is = new FileInputStream(new File("src/test/files/ingest.xml"));
        	byte[] bytes = IOUtils.toByteArray(is);
        	id = APIM.ingest(TypeUtility.convertBytesToDataHandler(bytes), Constants.FOXML1_1.uri, "ingesting new foxml object");
        }
        catch (IOException e) {
        	e.getMessage();
        }
        
 		try {
    		String fileName = id.substring(id.lastIndexOf(":")+1);
    		
 			DataHandler object = APIM.getObjectXML(id);
 			String contentType = object.getContentType();
 			assertEquals(contentType, "text/xml");
 			
			InputStream inputObject = object.getInputStream();
	 		
			MessageDigest digestObject = fedoraMessaging.createTempFile(inputObject, fileName);
			
            String fileChecksum = fedoraMessaging.checksumBytesToString(digestObject.digest());
            
		
			List <DatastreamDef> datastreamList = APIA.listDatastreams(id, null);
			assertNotNull(datastreamList);
			
			for (DatastreamDef datastreamDef : datastreamList) {
				String datastreamID = datastreamDef.getID();
				Datastream datastream = APIM.getDatastream(id, datastreamID, null);
				DatastreamControlGroup controlGroup = datastream.getControlGroup();
				String controlGroupType = controlGroup.name();
				String versionID = datastream.getVersionID();
				
				if (controlGroupType.equals("M")) {
					assertEquals(versionID, datastreamID + ".0");
					assertEquals(datastream.getLabel(), "test.pdf");
					
					MIMETypedStream stream = APIA.getDatastreamDissemination(id, datastreamID, null);
					DataHandler data = stream.getStream();
					InputStream inputData = data.getInputStream();
					assertEquals(stream.getMIMEType(), "application/pdf");
					
					String fullFilename = fileName + "+" + datastreamID + "+" + versionID;
					MessageDigest digestData = fedoraMessaging.createTempFile(inputData, fullFilename);
					
		            fileChecksum = fedoraMessaging.checksumBytesToString(digestData.digest());
				}	
			}
 		}
 		catch (Exception e) {
 			e.printStackTrace();
 		}
		        	
	}	

    @Test
 	public void writeFiles() {
	 	
    	FedoraMessaging fedoraMessaging = new FedoraMessaging();
    	
 		try {
	    	InputStream inputObject = new FileInputStream(new File("src/test/files/ingest.xml"));
	    	
			MessageDigest digestObject = createTempFile(inputObject, "ingest.xml");
			
	        String fileChecksum = fedoraMessaging.checksumBytesToString(digestObject.digest());
	        
			File upload = new File("src/test/files/ingest.xml");
			
	 	 	JCloudSwift jcloud = new JCloudSwift();
	 		assertNotNull(jcloud);
	   	 	
 		    jcloud.uploadObject("era/test", upload);
 		    
			MutableObjectInfoWithMetadata metadata = jcloud.getObjectInfo("era/test", upload.getName());
	
			String retrievedChecksum = fedoraMessaging.checksumBytesToString(metadata.getHash());
            
			assertEquals(retrievedChecksum, fileChecksum);
			
			long retrievedLength = metadata.getBytes();
			assertEquals(retrievedLength, upload.length());
			
			upload.delete();
 		}
 		catch (Exception e) {
 			e.printStackTrace();
 		}
	}
 	
    @Test
    public void testMintNoid() {
       	
	  	client = new HttpClient();
	  	assertNotNull(client);
	  	
    	client.getHttpConnectionManager().getParams().setConnectionTimeout(50000);
    	
    	HttpMethod method = new PostMethod("http://millwall.library.ualberta.ca/nd/noidu_dig?mint+1");
    	
    	String responseBody = null;
    	String noid = null;
    	try{
             client.executeMethod(method);
             int statusCode = method.getStatusCode();
             assertEquals(statusCode, 200);
             
             responseBody = method.getResponseBodyAsString();
	         noid = responseBody.substring(responseBody.indexOf(":")+2,responseBody.indexOf("\n"));
	         assertNotNull(noid);
       } 
    	catch (HttpException he) {
        	 he.getMessage();
        } 
    	catch (IOException e){
    		 e.getMessage();
             e.getStackTrace();
        }     
    	
    }
    
    public MessageDigest createTempFile(InputStream inputStream, String id) {
       	
    	MessageDigest digest = null;
    	
    	try {
        	digest = MessageDigest.getInstance("MD5");
        	
	        OutputStream outputStream = new FileOutputStream("tmp/" + id);
	        
	        byte[] buffer = new byte[1024];
	        int bytesRead = 0;
	        while((bytesRead = inputStream.read(buffer)) !=-1){
	        	outputStream.write(buffer, 0, bytesRead);
	        	digest.update(buffer, 0, bytesRead);
	        }
	        
	        inputStream.close();
	        outputStream.flush();
	        outputStream.close();
    	}    
	    catch (IOException e) { 
	    	e.getMessage();
	    }
	    catch (NoSuchAlgorithmException e) {    
	  		e.getMessage();
	    }
    	
    	return digest;
    }
    
}
