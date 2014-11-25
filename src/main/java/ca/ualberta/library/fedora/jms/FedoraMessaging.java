package ca.ualberta.library.fedora.jms;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpException;
import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.io.IOUtils;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.TextMessage;
import javax.naming.Context;
import javax.activation.DataHandler;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.security.MessageDigest;
import java.util.Iterator;
import java.util.Properties;
import java.util.List;
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
import org.fcrepo.server.types.gen.Datastream;
import org.fcrepo.server.types.gen.DatastreamControlGroup;
import org.fcrepo.server.types.gen.DatastreamDef;
import org.jclouds.openstack.swift.domain.MutableObjectInfoWithMetadata;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

import ca.ualberta.library.jclouds.JCloudSwift;

public class FedoraMessaging implements MessagingListener {
    MessagingClient messagingClient;
	private static final Log log = LogFactory.getLog(FedoraMessaging.class);
    private static FedoraAPIAMTOM APIA = null;
    private static FedoraAPIMMTOM APIM = null;
    private String swiftContainer = null;
    private String tmpDirectory = null;
    private String noidURL = null;
    private HttpClient client;
	
	public static void main(String[] args) {
		
		try {
			FedoraMessaging messaging = new FedoraMessaging();
			messaging.start();
		}
		catch (MessagingException e) {
	  		log.error(e.getMessage());
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
	  		log.error(e.getMessage());
	  	}
    	
    	Properties clientProperties = new Properties();
	  	try {
	  		clientProperties.load(new FileInputStream("client.properties"));
		  
		  	String baseURL = clientProperties.getProperty("baseURL");
		  	String username = clientProperties.getProperty("username");
		  	String password = clientProperties.getProperty("password");
		  	tmpDirectory = clientProperties.getProperty("tmpDirectory");
			FedoraClient fedoraClient = new FedoraClient(baseURL, username, password);
			APIA = fedoraClient.getAPIAMTOM();
			APIM = fedoraClient.getAPIMMTOM();
		}	  
	  	catch (Exception e) {
	  		log.error(e.getMessage());
	  	}
	  	
    	Properties jcloudProperties = new Properties();
	  	try {
	  		jcloudProperties.load(new FileInputStream("swift.properties"));
		  
		  	swiftContainer = jcloudProperties.getProperty("container");
	  	}	  
	  	catch (IOException e) {
	  		log.error(e.getMessage());
	  	}
    	
    	Properties noidProperties = new Properties();
	  	try {
	  		noidProperties.load(new FileInputStream("noid.properties"));
		  
		  	noidURL = noidProperties.getProperty("url");
		  	 
		  	client = new HttpClient();
	    	client.getHttpConnectionManager().getParams().setConnectionTimeout(50000);
	  	}	  
	  	catch (IOException e) {
	  		log.error(e.getMessage());
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
						log.info("Processing object: " + idText);
			    	}
			    	
			    	if (idText != null) {
				    	getFedoraObject(idText);
			    	}
	    		}
	    	}	
        } catch(JMSException e) {
            log.error("Error retrieving message text: " + e.getMessage());
	    } catch(IOException e) {
	        log.error("Error retrieving input stream: " + e.getMessage());
	    } catch(DocumentException e) {
	        log.error("Error parsing document: " + e.getMessage());
	    }
        
        log.info("Message received from client: " + clientId);
	}

 	public void getFedoraObject(String id) {
		
 		try {
    		String fileName = id.substring(id.lastIndexOf(":")+1);
    		
 			DataHandler object = APIM.getObjectXML(id);
			InputStream inputObject = object.getInputStream();
	 		
			MessageDigest digestObject = createTempFile(inputObject, fileName);
			
            String fileChecksum = checksumBytesToString(digestObject.digest());
            
            String noid = mintNoid();
            
			File uploadObject = new File(tmpDirectory + "/" + fileName);
			
			writeFiles(noid, uploadObject, fileChecksum);
			
			List <DatastreamDef> datastreamList = APIA.listDatastreams(id, null);
			for (DatastreamDef datastreamDef : datastreamList) {
				String datastreamID = datastreamDef.getID();
				Datastream datastream = APIM.getDatastream(id, datastreamID, null);
				DatastreamControlGroup controlGroup = datastream.getControlGroup();
				String controlGroupType = controlGroup.name();
				String versionID = datastream.getVersionID();
				
				if (controlGroupType.equals("M")) {
					MIMETypedStream stream = APIA.getDatastreamDissemination(id, datastreamID, null);
					DataHandler data = stream.getStream();
					InputStream inputData = data.getInputStream();
					
					String fullFilename = fileName + "+" + datastreamID + "+" + versionID;
					MessageDigest digestData = createTempFile(inputData, fullFilename);
					
		            fileChecksum = checksumBytesToString(digestData.digest());
		            
					File upload = new File("tmp/" + fullFilename);
					
					writeFiles(noid, upload, fileChecksum);
				}	
			}
 		}
 		catch (Exception e) {
	  		log.error(e.getMessage());
 		}
		        	
	}	

 	public void writeFiles(String noid, File upload, String fileChecksum) {
 		
 		try {
 	 	 	JCloudSwift jcloud = new JCloudSwift();
 	   	 	
			if (jcloud.uploadObject(swiftContainer + "/" + "testobject", upload)) {
				MutableObjectInfoWithMetadata metadata = jcloud.getObjectInfo(swiftContainer + "/" + "testobject", upload.getName());
		
				String retrievedChecksum = checksumBytesToString(metadata.getHash());
	
				if (!retrievedChecksum.equals(fileChecksum)) {
					log.error("Checksums not equal for: " + upload.getName());
				}
				
				long retrievedLength = metadata.getBytes();
				if (retrievedLength != upload.length()) {
					log.error("File size does not match for: " + upload.getName());
				}
				
				log.info("Object copied: " + upload.getName());
		 		
		 		jcloud.close();
		 		upload.delete();
			}
 		}
 		catch (Exception e) {
	  		log.error(e.getMessage());
 		}
	}
 	
    public MessageDigest createTempFile(InputStream inputStream, String id) {
       	
    	MessageDigest digest = null;
    	
    	try {
        	digest = MessageDigest.getInstance("MD5");
        	
	        OutputStream outputStream = new FileOutputStream(tmpDirectory + "/" + id);
	        
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
	    	log.error(e.getMessage());
	    }
	    catch (NoSuchAlgorithmException e) {    
	  		log.error(e.getMessage());
	    }
    	
    	return digest;
    }
    
    public String mintNoid() {
       	
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
    
    public static String checksumBytesToString(byte[] digestBytes) {
        
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