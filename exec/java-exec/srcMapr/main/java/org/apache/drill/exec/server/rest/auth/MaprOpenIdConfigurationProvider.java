package org.apache.drill.exec.server.rest.auth;

import org.apache.hadoop.security.authentication.util.SsoConfigurationUtil;

public class MaprOpenIdConfigurationProvider extends OpenIdConfigurationProvider {
  private final SsoConfigurationUtil ssoConfigurationUtil;

  public MaprOpenIdConfigurationProvider() {
    ssoConfigurationUtil = SsoConfigurationUtil.getInstance();
  }

  @Override
  public String getClientIssuer() {
    return ssoConfigurationUtil.getClientIssuer();
  }

  @Override
  public String getClientId() {
    return ssoConfigurationUtil.getClientId();
  }

  @Override
  public String getClientSecret() {
    return ssoConfigurationUtil.getClientSecret();
  }

  @Override
  public String getUserAttrName() {
    return ssoConfigurationUtil.getUserAttrName();
  }
}
