package org.apache.drill.exec.server.rest.auth;

import com.google.common.base.Strings;
import org.apache.drill.common.exceptions.DrillException;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.server.rest.WebServerConstants;
import org.eclipse.jetty.util.security.Constraint;

public class OpenIdSecurityHandler extends DrillHttpConstraintSecurityHandler {
  @Override
  public void doSetup(DrillbitContext dbContext) throws DrillException {
    OpenIdConfigurationProvider openIdConfigurationProvider =
        OpenIdConfigurationProvider.getProvider(dbContext.getConfig());
    validateOpenIdConfiguration(openIdConfigurationProvider);

    DrillOpenIdConfiguration oidcConfig = openIdConfigurationProvider.getConfiguration();
    String errorPage = WebServerConstants.MAIN_LOGIN_RESOURCE_PATH;
    String authenticationUri = WebServerConstants.OPEN_ID_LOGIN_RESOURCE_PATH;
    String redirectUri = authenticationUri + "/callback";
    String logoutRedirectPath = WebServerConstants.LOGOUT_RESOURCE_PATH;

    DrillOpenIdLoginService oidcLoginService = new DrillOpenIdLoginService(oidcConfig, dbContext,
        openIdConfigurationProvider.getUserAttrName());
    DrillOpenIdAuthenticator oidcAuthenticator = new DrillOpenIdAuthenticator(oidcConfig,
        authenticationUri, redirectUri, errorPage, logoutRedirectPath);

    setup(oidcAuthenticator, oidcLoginService);
  }

  @Override
  public String getImplName() {
    return Constraint.__OPENID_AUTH;
  }

  public void validateOpenIdConfiguration(OpenIdConfigurationProvider provider) {
    StringBuilder errorMessage = new StringBuilder()
        .append(provider.getClass().getName())
        .append(". ");
    if (Strings.isNullOrEmpty(provider.getClientIssuer())) {
      errorMessage.append("OIDC client issuer is not provided");
    } else if (Strings.isNullOrEmpty(provider.getClientId())) {
      errorMessage.append("OIDC client Id is not provided");
    } else if (Strings.isNullOrEmpty(provider.getClientSecret())) {
      errorMessage.append("OIDC client secret is not provided");
    } else if (Strings.isNullOrEmpty(provider.getUserAttrName())) {
      errorMessage.append("OIDC user attribute name is not provided");
    } else {
      return;
    }
    throw new IllegalArgumentException(errorMessage.toString());
  }
}
