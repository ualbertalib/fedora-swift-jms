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

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.javaswift.joss.client.factory.TempUrlHashPrefixSource;
import org.javaswift.joss.model.Access;


// Extend JOSS Access interface to include support for Openstack Keystone V3
public class KeystoneV3Access implements Access {

  /*
  * Logger
  */
  private static final Logger LOG = LoggerFactory.getLogger(KeystoneV3Access.class);


  /**
   * the region where you want to access the resources
   **/
  private String preferredRegion = null;

  /**
   * security token to pass to all secure ObjectStore calls
   **/
  private String token = null;

  /**
   * internal URL to access resources with
   **/
  private String internalURL = null;

  /**
   * public URL of a resource
   **/
  private String publicURL = null;

  /**
   * Parse Keystone V3 API response
   * Password Scoped Authentication
   *
   * @param jsonResponse response from the Keystone
   * @param headerToken security token, if present
   * @param preferredRegion  
   */
  public KeystoneV3Access(
      JSONObject jsonResponse,
      String headerToken,
      String preferredRegion 
      ) 
  {
 
    // set the security token 
    this.token = headerToken;

    // retrieve the internalURL and publicURL from the JSON response 
    JSONObject respToken = (JSONObject) jsonResponse.get("token");
    JSONArray catalog = (JSONArray) respToken.get("catalog");
    for (Object obj: catalog) {
      JSONObject jsonObj = (JSONObject) obj;
      String name = (String) jsonObj.get("name");
      String type = (String) jsonObj.get("type");
      if (name.equals("swift") && type.equals("object-store")) {
        JSONArray endPoints = (JSONArray) jsonObj.get("endpoints");
        for (Object endPointObj: endPoints) {
          JSONObject endPoint = (JSONObject) endPointObj;
          String region = (String) endPoint.get("region");
          // choose the public and internal URL from the preferred region
          if (region.equals(preferredRegion)) {
            String interfaceType = (String) endPoint.get("interface");
            if (interfaceType.equals("public")) {
              this.publicURL = (String) endPoint.get("url");
            } else if (interfaceType.equals("internal")) {
              this.internalURL = (String) endPoint.get("url");
            }
          }
        }
      }
    }
  }

  @Override
  public String getInternalURL() {
    return this.internalURL;
  }

  @Override
  public String getPublicURL() {
    return this.publicURL;
  }

  @Override
  public String getTempUrlPrefix(TempUrlHashPrefixSource arg0) {
    return null;
  }

  @Override
  public String getToken() {
    return this.token;
  }

  @Override
  public boolean isTenantSupplied() {
    return true;
  }

  @Override
  public void setPreferredRegion(String region) {
    this.preferredRegion = region;
  }

}
