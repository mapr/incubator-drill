package org.apache.drill.exec.server.rest.auth;

import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.server.options.SystemOptionManager;
import org.apache.drill.exec.util.ImpersonationUtil;
import org.eclipse.jetty.security.DefaultIdentityService;
import org.eclipse.jetty.security.IdentityService;
import org.eclipse.jetty.security.LoginService;
import org.eclipse.jetty.security.openid.OpenIdConfiguration;
import org.eclipse.jetty.security.openid.OpenIdCredentials;
import org.eclipse.jetty.security.openid.OpenIdUserPrincipal;
import org.eclipse.jetty.server.UserIdentity;
import org.eclipse.jetty.util.component.ContainerLifeCycle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import javax.security.auth.Subject;
import javax.servlet.ServletRequest;
import java.security.Principal;
import java.util.Map;
import java.util.Set;

public class DrillOpenIdLoginService extends ContainerLifeCycle implements LoginService {
  private static final Logger logger = LoggerFactory.getLogger(DrillOpenIdLoginService.class);
  private final OpenIdConfiguration configuration;
  private final DrillbitContext drillContext;
  private final String userAttributeName;
  private IdentityService identityService = new DefaultIdentityService();

  public DrillOpenIdLoginService(OpenIdConfiguration configuration, DrillbitContext context, String userAttributeName) {
    this.drillContext = context;
    this.configuration = configuration;
    this.userAttributeName = userAttributeName;
    addBean(this.configuration);
  }


  @Override
  public String getName() {
    return configuration.getIssuer();
  }

  @Override
  public boolean validate(UserIdentity user) {
    return true;
  }

  @Override
  public IdentityService getIdentityService() {
    return identityService;
  }

  @Override
  public void setIdentityService(IdentityService service) {
    this.identityService = service;
  }

  @Override
  public UserIdentity login(String identifier, Object credentials, ServletRequest req) {
    OpenIdCredentials openIdCredentials = (OpenIdCredentials) credentials;
    try {
      openIdCredentials.redeemAuthCode(configuration);
    } catch (Throwable e) {
      logger.warn(e.getMessage());
      return null;
    }

    String username = getUsername(openIdCredentials);
    MDC.put("drill.userName", username);

    boolean isAdmin = isAdmin(username);

    final Principal drillUser = new DrillUserPrincipal(username, isAdmin);
    OpenIdUserPrincipal openIdUser = new OpenIdUserPrincipal(openIdCredentials);

    final Subject subject = new Subject();
    Set<Principal> principals = subject.getPrincipals();
    principals.add(drillUser);
    principals.add(openIdUser);
    subject.getPrivateCredentials().add(credentials);
    subject.setReadOnly();

    logger.info("WebUser {} logged in from {}:{}", username, req.getRemoteHost(), req.getRemotePort());
    if (isAdmin) {
      return identityService.newUserIdentity(subject, drillUser, DrillUserPrincipal.ADMIN_USER_ROLES);
    } else {
      return identityService.newUserIdentity(subject, drillUser, DrillUserPrincipal.NON_ADMIN_USER_ROLES);
    }
  }

  private String getUsername(OpenIdCredentials openIdCredentials) {
    Map<String, Object> claims = openIdCredentials.getClaims();
    Object username = claims.get(userAttributeName);
    logger.debug("UserId {} has username claim {}={}.", openIdCredentials.getUserId(), userAttributeName, username);
    return (String) username;
  }

  private boolean isAdmin(String userName) {
    SystemOptionManager sysOptions = drillContext.getOptionManager();
    return ImpersonationUtil.hasAdminPrivileges(userName,
        ExecConstants.ADMIN_USERS_VALIDATOR.getAdminUsers(sysOptions),
        ExecConstants.ADMIN_USER_GROUPS_VALIDATOR.getAdminUserGroups(sysOptions));
  }

  @Override
  public void logout(UserIdentity user) {
    MDC.clear();
    if (logger.isTraceEnabled()) {
      logger.trace("Web user {} logged out.", user.getUserPrincipal().getName());
    }
  }
}
