package org.apache.drill.exec.server.rest.auth;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.ExecConstants;
import org.eclipse.jetty.security.openid.OpenIdConfiguration;

public abstract class OpenIdConfigurationProvider {
  public static OpenIdConfigurationProvider getProvider(DrillConfig config) {
    return new DrillProperties(config);
  }

  public OpenIdConfiguration getConfiguration() {
    return new OpenIdConfiguration(getClientIssuer(), getClientId(), getClientSecret());
  }

  @Override
  public boolean equals(Object obj) {
    return super.equals(obj);
  }

  public abstract String getClientIssuer();

  public abstract String getClientId();

  public abstract String getClientSecret();

  public abstract String getUserAttrName();

  static class DrillProperties extends OpenIdConfigurationProvider {
    private DrillConfig config;
    private DrillProperties(DrillConfig config) {
      this.config = config;
    }

    @Override
    public String getClientIssuer() {
      return config.getString(ExecConstants.OIDC_PROVIDER_ENDPOINT);
    }

    @Override
    public String getClientId() {
      return config.getString(ExecConstants.OIDC_CLIENT_ID);
    }

    @Override
    public String getClientSecret() {
      return config.getString(ExecConstants.OIDC_CLIENT_SECRET);
    }

    @Override
    public String getUserAttrName() {
      return config.getString(ExecConstants.OIDC_USER_CLAIM);
    }
  }
}
