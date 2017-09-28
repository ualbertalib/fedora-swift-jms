package ca.ualberta.library.fedora.jms.keystone_v3;

/* 
 * External JOSS authentication mechanism due to lack of support for 
 * Openstack Keystone V3 as of 2017-09-07
 *
 * https://github.com/javaswift/joss/issues/112
 * https://github.com/javaswift/joss/issues/133
 * https://docs.openstack.org/keystone/ocata/devref/api_curl_examples.html
 *                                                             
**/

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;

import java.net.HttpURLConnection;
import java.net.URL;

import org.javaswift.joss.client.factory.AuthenticationMethod.AccessProvider;
import org.javaswift.joss.model.Access;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ca.ualberta.library.fedora.jms.keystone_v3.KeystoneV3Access;


public class KeystoneV3AccessProvider implements AccessProvider {

  /*
  * Logger
  */
  private static final Logger LOG = LoggerFactory .getLogger(KeystoneV3AccessProvider.class);

  private String username = null;
  private String password = null;
  private String projectId = null;
  private String authUrl = null;
  private String domainName = null;  

  /**
   * Constructor: Keystone V3 auth
   **/ 
  public KeystoneV3AccessProvider(
      String username,
      String password,
      String authUrl,
      String projectId,
      String domainName) 
  {
    this.username = username;
    this.password = password;
    this.authUrl = authUrl;
    this.projectId = projectId;
    this.domainName = domainName; 
  }

  @Override
  public Access authenticate() {
    try {
      return keystoneV3Auth();
    } catch (IOException e) {
      LOG.error(e.getMessage());
      return null;
    }
  }


  /**
   * Openstack Keystonve V3 authentication logic
   *
   * @return Access JOSS access object
   * @throws IOException if failed to parse the response
   **/

  public Access keystoneV3Auth() throws IOException {

		InputStreamReader reader = null;
    BufferedReader bufReader = null;

    try {
      // build JSON request body for Keystone V3 authentication
      
      // user key
      JSONObject user = new JSONObject();
      user.put("id", this.username);
      user.put("password", this.password);
      JSONObject password = new JSONObject();
      password.put("user", user);

      // methods key
      JSONArray methods = new JSONArray();
      methods.add("password");
    
      // identity key 
      JSONObject identity = new JSONObject();
      identity.put("methods", methods);
      identity.put("password", password);
    
      // scope key 
      JSONObject project = new JSONObject();
      project.put("id", this.projectId);
      JSONObject scope = new JSONObject();
      scope.put("project", project);
      
      // auth key 
      JSONObject auth = new JSONObject();
      auth.put("identity", identity);
      auth.put("scope", scope);

      JSONObject requestBody = new JSONObject();
      requestBody.put("auth", auth);

      // connect to Keystone V3 server
      HttpURLConnection con =
          (HttpURLConnection) new URL(this.authUrl).openConnection();
      con.setDoOutput(true);
      con.setRequestProperty("Accept", "application/json");
      con.setRequestProperty("Content-Type", "application/json");
      OutputStream output = con.getOutputStream();
      output.write(requestBody.toString().getBytes());
      int status = con.getResponseCode();
      if (status != 201) {
        throw new IOException(
            "unexpected response code:" + status + " " + con.getResponseMessage()
            );
      }
      
      // if successful, grab response
      reader = new InputStreamReader(con.getInputStream());
      bufReader = new BufferedReader(reader);
      String response = bufReader.readLine();
      JSONParser parser = new JSONParser();
      JSONObject jsonResponse = (JSONObject) parser.parse(response);

      // parse response into Access object
      String token = con.getHeaderField("X-Subject-Token");
      KeystoneV3Access access = new KeystoneV3Access(jsonResponse, token, "");

      // clean-up
      con.disconnect();
      return access;

    } catch (Exception e) {
      throw new IOException(e);
    } finally {
      // clean-up
      if (bufReader != null) {
        bufReader.close();
      }
      if (reader != null) {
        reader.close();
      }
    }
  }    


}
