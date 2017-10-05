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

import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

import org.javaswift.joss.client.factory.AccountConfig;
import org.javaswift.joss.client.factory.AccountFactory;
import org.javaswift.joss.client.factory.AuthenticationMethod;
import org.javaswift.joss.model.Account;
import org.javaswift.joss.model.Container;
import org.javaswift.joss.model.StoredObject;

import ca.ualberta.library.fedora.jms.keystone_v3.KeystoneV3AccessProvider;


public class FedoraMessaging implements MessagingListener {

    MessagingClient messagingClient;
    private static final Log log = LogFactory.getLog(FedoraMessaging.class);
    private static FedoraAPIAMTOM APIA = null;
    private static FedoraAPIMMTOM APIM = null;
    private String swiftContainer = null;
    private Account swiftAccount = null;
    private String tmpDirectory = null;
    private String noidURL = null;
    private HttpClient client;

    public static void main(String[] args) {

        try {
            FedoraMessaging messaging = new FedoraMessaging();
            messaging.start();
        } catch (MessagingException e) {
            log.error(e.getMessage());
        }
    }

    public void start() throws MessagingException {

        Properties clientProperties = new Properties();
        try {
            clientProperties.load(new FileInputStream("client.properties"));

            String baseURL = clientProperties.getProperty("baseURL");
            String username = clientProperties.getProperty("username");
            String password = clientProperties.getProperty("password");


            tmpDirectory = clientProperties.getProperty("tmpDirectory");
            
            FedoraClient fedoraClient = new FedoraClient(baseURL, username, password);
            log.debug("Connect to Fedora: " + baseURL + " " + username + " " + password + " " + tmpDirectory);

            APIA = fedoraClient.getAPIAMTOM();
            APIM = fedoraClient.getAPIMMTOM();
            
        } catch (Exception e) {
            log.error("Connect to Fedora: " + e.getMessage());
        }

        Properties swiftProperties = new Properties();
        try {
            swiftProperties.load(new FileInputStream("swift.properties"));

            AccountConfig swiftConfig = new AccountConfig();
            swiftConfig.setUsername(swiftProperties.getProperty("identity"));
            swiftConfig.setPassword(swiftProperties.getProperty("password"));
            swiftConfig.setAuthUrl(swiftProperties.getProperty("endpoint"));
            swiftConfig.setTenantName(swiftProperties.getProperty("tenant"));
            swiftConfig.setPreferredRegion(swiftProperties.getProperty("preferredRegion"));

            // External access provide for Keystone v3 support - 2017-09-27
            // No Keystone v3 support yet in JOSS or jCloud Swift connectors
            swiftConfig.setAuthenticationMethod(AuthenticationMethod.EXTERNAL);

            KeystoneV3AccessProvider externalAccessProvider =
              new KeystoneV3AccessProvider(
                  swiftConfig.getAuthUrl(),
                  swiftConfig.getUsername(),
                  swiftConfig.getPassword(),
                  swiftProperties.getProperty("userDomainId"),
                  swiftProperties.getProperty("projectName"),
                  swiftProperties.getProperty("projectDomainId"),
                  swiftConfig.getPreferredRegion()
                  );
            swiftConfig.setAccessProvider(externalAccessProvider);

            swiftAccount = new AccountFactory(swiftConfig).createAccount();

            swiftContainer = swiftProperties.getProperty("container");
        } catch (IOException e) {
            log.error("Connect to Swift: " + e.getMessage());
        }

        Properties noidProperties = new Properties();
        try {
            noidProperties.load(new FileInputStream("noid.properties"));

            noidURL = noidProperties.getProperty("url");

            client = new HttpClient();
            client.getHttpConnectionManager().getParams().setConnectionTimeout(50000);
        } catch (IOException e) {
            log.error(e.getMessage());
        }
        
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
        } catch (IOException e) {
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
            messageText = ((TextMessage) message).getText();

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
        } catch (JMSException e) {
            log.error("Error retrieving message text: " + e.getMessage());
        } catch (IOException e) {
            log.error("Error retrieving input stream: " + e.getMessage());
        } catch (DocumentException e) {
            log.error("Error parsing document: " + e.getMessage());
        }

        log.info("Message received from client: " + clientId);
    }

    public void getFedoraObject(String id) {

        try {
            String fileName = id.substring(id.lastIndexOf(":") + 1);
            String tmpFileName = tmpDirectory + "/" + fileName;

            DataHandler object = APIM.getObjectXML(id);
            InputStream inputObject = object.getInputStream();

            MessageDigest digestObject = createTempFile(inputObject, tmpFileName);

            String fileChecksum = checksumBytesToString(digestObject.digest());

            String noid = mintNoid();

            File uploadObject = new File(tmpFileName);

            writeFiles(noid, uploadObject, fileChecksum);

            List<DatastreamDef> datastreamList = APIA.listDatastreams(id, null);
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
                    String tmpFullFilename = tmpDirectory + "/" + fullFilename;
                    MessageDigest digestData = createTempFile(inputData, tmpFullFilename);

                    fileChecksum = checksumBytesToString(digestData.digest());

                    File upload = new File(tmpFullFilename);

                    writeFiles(noid, upload, fileChecksum);
                }
            }
        } catch (Exception e) {
            log.error(e.getMessage());
        }

    }

    public void writeFiles(String noid, File upload, String fileChecksum) {

        try {
            log.info("Upload Object From File" + upload.getName());

            Container container = swiftAccount.getContainer(swiftContainer);
            StoredObject object = container.getObject(upload.getName());
            object.uploadObject(upload);

            if (object.exists()) {

                String retrievedChecksum = object.getEtag();
                if (!retrievedChecksum.equals(fileChecksum)) {
                    log.error("Checksums not equal for: " + upload.getName());
                }

                long retrivedLength = object.getContentLength();
                if (retrivedLength != upload.length()) {
                    log.error("File size does not match for: " + upload.getName());
                }

                log.info("Object copied: " + upload.getName());

                upload.delete();
            }
        } catch (Exception e) {
            log.error(e.getMessage());
        }
    }

    public String getFileChecksum(MessageDigest digest, File upload) throws IOException {

				StringBuilder sb = null; 
				String ret = null;
        try {
            FileInputStream inputStream = new FileInputStream(upload);
            byte[] buffer = new byte[1024];
            int bytesRead = 0;

						while ((bytesRead = inputStream.read(buffer)) != -1) {
								digest.update(buffer, 0, bytesRead);
						};
						inputStream.close();
						
						//Get the hash's bytes
						byte[] bytes = digest.digest();
						ret = checksumBytesToString(bytes);
        } catch (IOException e) {
            log.error(e.getMessage());
				}
			  return ret;
    }

    public MessageDigest createTempFile(InputStream inputStream, String id) {

        MessageDigest digest = null;

        try {
            digest = MessageDigest.getInstance("MD5");

            OutputStream outputStream = new FileOutputStream(id);

            byte[] buffer = new byte[1024];
            int bytesRead = 0;
            while ((bytesRead = inputStream.read(buffer)) != -1) {
                outputStream.write(buffer, 0, bytesRead);
                digest.update(buffer, 0, bytesRead);
            }

            inputStream.close();
            outputStream.flush();
            outputStream.close();
        } catch (IOException e) {
            log.error(e.getMessage());
        } catch (NoSuchAlgorithmException e) {
            log.error(e.getMessage());
        }

        return digest;
    }

    public String mintNoid() {

        HttpMethod method = new PostMethod(noidURL);

        String responseBody = null;
        String noid = null;
        try {
            client.executeMethod(method);
            responseBody = method.getResponseBodyAsString();
            noid = responseBody.substring(responseBody.indexOf(":") + 2, responseBody.indexOf("\n"));
        } catch (HttpException he) {
            log.error("Http error connecting to '" + noidURL + "'");
            log.error(he.getMessage());
        } catch (IOException e) {
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
        for (int i = 0; i < digestBytes.length; i++) {
            String hex = Integer.toHexString(0xff & digestBytes[i]);
            if (hex.length() == 1) {
                hexString.append('0');
            }
            hexString.append(hex);
        }

        return hexString.toString();
    }

    public void setTmpDirectory(String s) {
      this.tmpDirectory = s;
    }

}
