package ca.ualberta.library.fedora.jms;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.*;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpException;
import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.Header;
import org.apache.commons.httpclient.methods.PostMethod;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.TextMessage;
import javax.naming.Context;
import javax.swing.event.MenuEvent;
import javax.swing.event.MenuListener;
import javax.activation.DataHandler;
import javax.xml.rpc.ServiceException;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.security.MessageDigest;
import java.util.Collection;
import java.util.Iterator;
import java.util.Properties;
import java.util.List;
import java.net.URL;
import java.security.NoSuchAlgorithmException;

import org.fcrepo.client.messaging.JmsMessagingClient;
import org.fcrepo.client.messaging.MessagingClient;
import org.fcrepo.client.messaging.MessagingListener;
import org.fcrepo.client.FedoraClient;
import org.fcrepo.server.messaging.JMSManager;
import org.fcrepo.server.errors.MessagingException;
import org.fcrepo.server.management.FedoraAPIMMTOM;
import org.fcrepo.server.access.FedoraAPIAMTOM;
import org.fcrepo.server.types.mtom.gen.MIMETypedStream;
import org.fcrepo.server.types.gen.DatastreamDef;
import org.fcrepo.common.Constants;

import org.jclouds.openstack.swift.domain.MutableObjectInfoWithMetadata;

import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;
import org.dom4j.Element;
import org.dom4j.Node;

import ca.ualberta.library.jclouds.JCloudSwift;

public class FedoraMessaging implements MessagingListener {
    MessagingClient messagingClient;
	private static final Log log = LogFactory.getLog(FedoraMessaging.class);
    private static FedoraAPIAMTOM APIA = null;
    private static FedoraAPIMMTOM APIM = null;
    private String swiftContainer = null;
    private String noidURL = null;
    private HttpClient client;
	
	public static void main(String[] args) {
		
		try {
			FedoraMessaging messaging = new FedoraMessaging();
			messaging.start();
		}
		catch (MessagingException e) {
			e.getMessage();
		}
	}
	
    public void start() throws MessagingException {
    	
    	Properties jmsProperties = new Properties();
	  	try {
	  		jmsProperties.load(new FileInputStream("jms.properties"));
		  
		  	String factory = jmsProperties.getProperty("initialContextFactory");
		  	String url = jmsProperties.getProperty("providerURL");
		  	String factoryName = jmsProperties.getProperty("connectionFactoryName");
		  	String messagingType = jmsProperties.getProperty("messagingType");
		  	String activityType = jmsProperties.getProperty("activityType");
		  	String messagingClientName = jmsProperties.getProperty("messagingClientName");
	    	
	        Properties properties = new Properties();
	        properties.setProperty(Context.INITIAL_CONTEXT_FACTORY, factory);
	        properties.setProperty(Context.PROVIDER_URL, url);
	        properties.setProperty(JMSManager.CONNECTION_FACTORY_NAME, factoryName);
	        properties.setProperty(messagingType, activityType);
	        messagingClient = new JmsMessagingClient(messagingClientName, this, properties, false);
	        messagingClient.start();
	  	}	  
	  	catch (IOException e) {
		  	e.printStackTrace();
	  	}
    	
    	Properties clientProperties = new Properties();
	  	try {
	  		clientProperties.load(new FileInputStream("client.properties"));
		  
		  	String baseURL = clientProperties.getProperty("baseURL");
		  	String username = clientProperties.getProperty("username");
		  	String password = clientProperties.getProperty("password");
			FedoraClient fedoraClient = new FedoraClient(baseURL, "fedoraAdmin", "fedoraAdmin");
			APIA = fedoraClient.getAPIAMTOM();
			APIM = fedoraClient.getAPIMMTOM();
		}	  
	  	catch (Exception e) {
		  	e.printStackTrace();
	  	}
	  	
    	Properties jcloudProperties = new Properties();
	  	try {
	  		jcloudProperties.load(new FileInputStream("swift.properties"));
		  
		  	swiftContainer = jcloudProperties.getProperty("container");
	  	}	  
	  	catch (IOException e) {
		  	e.printStackTrace();
	  	}
    	
    	Properties noidProperties = new Properties();
	  	try {
	  		noidProperties.load(new FileInputStream("noid.properties"));
		  
		  	noidURL = noidProperties.getProperty("url");
		  	 
		  	client = new HttpClient();
	    	client.getHttpConnectionManager().getParams().setConnectionTimeout(50000);
	  	}	  
	  	catch (IOException e) {
		  	e.printStackTrace();
	  	}
   }
    
    public void stop() throws MessagingException {
        messagingClient.stop(false);
    }
    
	public void onMessage(String clientId, Message message) {
		
		Document document = null;
        SAXReader reader = new SAXReader();
        
        String messageText = "";
        try {
            messageText = ((TextMessage)message).getText();
            
            InputStream inputStream = IOUtils.toInputStream(messageText, "UTF-8");
            document = reader.read(inputStream);
            String doc = document.asXML();
            
		    Element rootElement = document.getRootElement();

		    String titleText = null; 
	    	Iterator<Element> titleIterator = rootElement.elementIterator("title");
	    	while (titleIterator.hasNext()) {
	    		Element title = titleIterator.next();
	    		titleText = title.getText();
	    	}
	    	
	    	String idText = null;
	    	if (titleText != null) {
	    		if (titleText.equals("ingest")) {
			    	Iterator<Element> idIterator = rootElement.elementIterator("summary");
			    	while (idIterator.hasNext()) {
			    		Element id = idIterator.next();
			    		idText = id.getText();
						log.info("ID: " + idText);
			    	}
			    	
			    	if (idText != null) {
				    	getFile(idText);
			    	}
	    		}
	    	}	
	    	
//			Node elementNode = (Node) document;
//			Element title = (Element) elementNode.selectSingleNode("entry/title");
//		    id.addNamespace("atom", "http://www.w3.org/2005/Atom");
//		    id.addNamespace("fedora-types", "http://www.fedora.info/definitions/1/0/types/");
//			String titleText = title.getText();
//			log.info("Title: " + titleText);
	    	
        } catch(JMSException e) {
            log.info("Error retrieving message text: " + e.getMessage());
	    } catch(IOException e) {
	        log.info("Error retrieving input stream: " + e.getMessage());
	    } catch(DocumentException e) {
	        log.info("Error parsing document: " + e.getMessage());
	    }
        
        log.info("Message received: " + messageText + " from client " + clientId);
	}

 	public void getFile(String id) {
		
//   	 	JCloudSwift jcloud = new JCloudSwift();
   	 	
		List <DatastreamDef> datastreamList = APIA.listDatastreams(id, null);
		for (DatastreamDef datastreamDef : datastreamList) {
			
			try {
				String datastreamID = datastreamDef.getID();
				
				MIMETypedStream stream = APIA.getDatastreamDissemination(id, datastreamID, null);
				DataHandler data = stream.getStream();
				InputStream inputStream = data.getInputStream();
				
	    		id = id.substring(id.lastIndexOf(":")+1);
	    		
				MessageDigest digest = createTempFile(inputStream, id);
				
	            String fileChecksum = checksumBytesToString(digest.digest());
	            
//	            String noid = mintNoid();
				
				File upload = new File("tmp" + id);
				
/*    			if (jcloud.uploadObject(swiftContainer + noid, upload)) {
    				MutableObjectInfoWithMetadata metadata = jcloud.getObjectInfo(swiftContainer + noid, upload.getName());
    			
	                String fileChecksum = checksumBytesToString(digest.digest());
	                
    				String retrievedChecksum = checksumBytesToString(metadata.getHash());
    		
    				if (!retrievedChecksum.equals(fileChecksum)) {
    					log.error("Checksums not equal for: " + upload.getName());
    				}
    				
    				long retrievedLength = metadata.getBytes();
    				if (retrievedLength != upload.length()) {
    					log.error("File size does not match for: " + upload.getName());
    				}
    				
    				log.info("Object copied: " + upload.getName());
    			}*/
	        }
			catch (IOException e) {
				e.printStackTrace();
			}
		}	
		        	
	}	

    private MessageDigest createTempFile(InputStream inputStream, String id) {
       	
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
	        
/*	        InputStream digestStream = new FileInputStream("tmp/" + id);
	        
	        bytesRead = 0;
	        while((bytesRead = digestStream.read(buffer)) !=-1){
	        	digest.update(buffer, 0, bytesRead);
	        }
	        
	        digestStream.close();*/
    	}    
	    catch (IOException e) { 
	    	log.error(e.getMessage());
			e.printStackTrace();
	    }
	    catch (NoSuchAlgorithmException e) {    
			e.printStackTrace();
	    }
    	
    	return digest;
    }
    
    private String mintNoid() {
       	
    	HttpMethod method = new PostMethod(noidURL);
    	
    	String responseBody = null;
    	String noid = null;
    	try{
             client.executeMethod(method);
             responseBody = method.getResponseBodyAsString();
	         noid = responseBody.substring(responseBody.indexOf(":")+2,responseBody.indexOf("\n"));
       } 
    	catch (HttpException he) {
        	 log.error("Http error connecting to '" + noidURL + "'");
        	 log.error(he.getMessage());
        } 
    	catch (IOException e){
    		 log.error(e.getMessage());
             e.getStackTrace();
        }     
    	
    	return noid;
    }
    
    private int readFromStream(InputStream inStream, byte[] buf) {
        
    	int numRead = -1;
        
        try {
            numRead = inStream.read(buf);
        } catch (IOException e) {
            log.error("Error reading stream", e);
            throw new RuntimeException(e);
        }
        
        return numRead;
    }
    
    private static String checksumBytesToString(byte[] digestBytes) {
        
    	StringBuffer hexString = new StringBuffer();
        for (int i=0; i<digestBytes.length; i++) {
            String hex=Integer.toHexString(0xff & digestBytes[i]);
            if(hex.length() == 1) {
                hexString.append('0');
            }
            hexString.append(hex);
        }
        
        return hexString.toString();
    }
}	