package org.apache.drill.exec.server.rest.auth;

import org.apache.drill.common.exceptions.DrillException;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.server.rest.WebServerConstants;
import org.eclipse.jetty.security.openid.OpenIdConfiguration;
import org.eclipse.jetty.util.security.Constraint;

public class OpenIdSecurityHandler extends DrillHttpConstraintSecurityHandler {
  @Override
  public void doSetup(DrillbitContext dbContext) throws DrillException {
    OpenIdConfigurationProvider openIdConfigurationProvider =
        OpenIdConfigurationProvider.getProvider(dbContext.getConfig());
    OpenIdConfiguration oidcConfig = openIdConfigurationProvider.getConfiguration();
    String errorPage = WebServerConstants.MAIN_LOGIN_RESOURCE_PATH;
    String authenticationUri = WebServerConstants.OPEN_ID_LOGIN_RESOURCE_PATH;
    String redirectUri = authenticationUri + "/callback";

    DrillOpenIdLoginService oidcLoginService = new DrillOpenIdLoginService(oidcConfig, dbContext,
        openIdConfigurationProvider.getUserAttrName());
    DrillOpenIdAuthenticator oidcAuthenticator = new DrillOpenIdAuthenticator(oidcConfig,
        authenticationUri, redirectUri, errorPage);

    setup(oidcAuthenticator, oidcLoginService);
  }

  @Override
  public String getImplName() {
    return Constraint.__OPENID_AUTH;
  }
}
