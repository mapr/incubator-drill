package org.apache.drill.exec.server.rest.auth;

import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.security.openid.OpenIdConfiguration;
import org.eclipse.jetty.util.ajax.JSON;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.stream.Collectors;

/**
 * Extends {@link OpenIdConfiguration} to include additional information about the Keycloak end
 * session endpoint. This class should be replaced with Jetty's implementation once Jetty is
 * updated to at least version 10.x.
 */
public class DrillOpenIdConfiguration extends OpenIdConfiguration {
  private static final Logger logger = LoggerFactory.getLogger(DrillOpenIdConfiguration.class);
  private static final String CONFIG_PATH = "/.well-known/openid-configuration";
  private static final String END_SESSION_ENDPOINT_METADATA_KEY = "end_session_endpoint";

  private String endSessionEndpoint;

  public DrillOpenIdConfiguration(String provider, String clientId, String clientSecret) {
    super(provider, null, null, clientId, clientSecret, null);
  }

  public DrillOpenIdConfiguration(String issuer, String authorizationEndpoint,
      String tokenEndpoint, String clientId, String clientSecret, String authMethod,
      String endSessionEndpoint,
      HttpClient httpClient) {
    super(issuer, authorizationEndpoint, tokenEndpoint, clientId, clientSecret, authMethod,
        httpClient);
    this.endSessionEndpoint = endSessionEndpoint;
  }

  @Override
  protected void doStart() throws Exception {
    super.doStart();
    if (endSessionEndpoint == null) {
      endSessionEndpoint = fetchOpenIdEndSessionEndpoint();
    }
  }

  protected String fetchOpenIdEndSessionEndpoint() {
    String issuer = getIssuer();
    HttpClient httpClient = getHttpClient();
    Map<String, Object> discoveryDocument = fetchOpenIdConnectMetadata(issuer, httpClient);

    String endSessionEndpoint = (String) discoveryDocument.get(END_SESSION_ENDPOINT_METADATA_KEY);
    logger.debug("Fetched end session endpoint: {}", endSessionEndpoint);
    return endSessionEndpoint;
  }

  /**
   * This is a full copy of the {@link OpenIdConfiguration} method.
   * We had to copy it because it has a private access modifier.
   */
  protected static Map<String, Object> fetchOpenIdConnectMetadata(String provider,
      HttpClient httpClient) {
    try {
      if (provider.endsWith("/")) {
        provider = provider.substring(0, provider.length() - 1);
      }

      Map<String, Object> result;
      String responseBody = httpClient.GET(provider + CONFIG_PATH)
          .getContentAsString();
      Object parsedResult = JSON.parse(responseBody);

      if (parsedResult instanceof Map) {
        Map<?, ?> rawResult = (Map) parsedResult;
        result = rawResult.entrySet().stream()
            .collect(Collectors.toMap(it -> it.getKey().toString(), Map.Entry::getValue));
      } else {
        logger.warn("OpenID provider did not return a proper JSON object response. Result was '{}'",
            responseBody);
        throw new IllegalStateException("Could not parse OpenID provider's malformed response");
      }

      logger.debug("discovery document {}", result);

      return result;
    } catch (Exception e) {
      throw new IllegalArgumentException("invalid identity provider", e);
    }
  }

  public String getEndSessionEndpoint() {
    return endSessionEndpoint;
  }
}
