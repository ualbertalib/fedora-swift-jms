package ca.ualberta.library.fedora.jms;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.util.List;
import java.util.Properties;

import javax.activation.DataHandler;
import javax.xml.rpc.ServiceException;

import org.fcrepo.server.access.FedoraAPIAMTOM;
import org.fcrepo.server.management.FedoraAPIMMTOM;
import org.fcrepo.server.types.gen.DatastreamDef;
import org.fcrepo.server.types.mtom.gen.MIMETypedStream;
import org.fcrepo.client.FedoraClient;
import org.fcrepo.common.Constants;



public class FedoraClientTest {

//	private static FedoraClient client;
//	private static FedoraCredentials credentials;
	
    public static FedoraAPIAMTOM APIA = null;

    public static FedoraAPIMMTOM APIM = null;

	public static void main(String[] args) {
		
		try { 
        	String URL = "http://localhost:8080/fedora";
			FedoraClient fc = new FedoraClient(URL, "fedoraAdmin", "fedoraAdmin");
			APIA = fc.getAPIAMTOM();
			APIM = fc.getAPIMMTOM();
		}
		catch (Exception e) {
			e.printStackTrace();
		}	
        
		try {
			DataHandler object = APIM.getObjectXML("changeme:1");
			InputStream in = object.getInputStream();
            OutputStream os = new FileOutputStream("test.xml");
            
            byte[] buffer = new byte[1024];
            int bytesRead;
            //read from is to buffer
            while((bytesRead = in.read(buffer)) !=-1){
                os.write(buffer, 0, bytesRead);
            }
            in.close();
            //flush OutputStream to write any buffered data to file
            os.flush();
            os.close();
			
			List <DatastreamDef> datastreamList = APIA.listDatastreams("changeme:1", null);
			for (DatastreamDef datastreamDef : datastreamList) {
					String datastreamID = datastreamDef.getID();
					String fileID = datastreamDef.getLabel();
					
					MIMETypedStream stream = APIA.getDatastreamDissemination("changeme:1", datastreamID, null);
					DataHandler data = stream.getStream();
					InputStream input = data.getInputStream();
					
		            OutputStream output = new FileOutputStream(fileID);
		            
		            //read from is to buffer
		            while((bytesRead = input.read(buffer)) !=-1){
		                output.write(buffer, 0, bytesRead);
		            }
		            input.close();
		            //flush OutputStream to write any buffered data to file
		            output.flush();
		            output.close();
			}            
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
//		  	credentials = new FedoraCredentials(new URL(baseURL), username, password);
//		  	client = new FedoraClient(credentials);
//		  	client.getClass();
		}	  
	  	catch (IOException e) {
		  	e.printStackTrace();
	  	}
	}
}
