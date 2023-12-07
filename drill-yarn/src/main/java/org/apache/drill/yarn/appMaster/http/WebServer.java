/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.yarn.appMaster.http;

import static org.apache.drill.exec.server.rest.auth.DrillUserPrincipal.ADMIN_ROLE;

import java.security.Principal;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Set;

import javax.servlet.DispatcherType;
import javax.servlet.ServletRequest;
import javax.servlet.http.HttpSession;
import javax.servlet.http.HttpSessionEvent;
import javax.servlet.http.HttpSessionListener;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.drill.exec.server.rest.CsrfTokenInjectFilter;
import org.apache.drill.exec.server.rest.CsrfTokenValidateFilter;
import org.apache.drill.exec.server.rest.ssl.SslContextFactoryConfigurator;
import com.google.common.collect.ImmutableSet;
import org.apache.drill.yarn.appMaster.Dispatcher;
import org.apache.drill.yarn.core.DrillOnYarnConfig;
import org.eclipse.jetty.http.HttpVersion;
import org.eclipse.jetty.security.ConstraintSecurityHandler;
import org.eclipse.jetty.security.DefaultIdentityService;
import org.eclipse.jetty.security.DefaultUserIdentity;
import org.eclipse.jetty.security.IdentityService;
import org.eclipse.jetty.security.LoginService;
import org.eclipse.jetty.security.SecurityHandler;
import org.eclipse.jetty.security.authentication.FormAuthenticator;
import org.eclipse.jetty.security.authentication.SessionAuthentication;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.SecureRequestCustomizer;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.SslConnectionFactory;
import org.eclipse.jetty.server.UserIdentity;
import org.eclipse.jetty.server.handler.ErrorHandler;
import org.eclipse.jetty.server.session.SessionHandler;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.resource.Resource;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.glassfish.jersey.servlet.ServletContainer;

import com.typesafe.config.Config;

/**
 * Wrapper around the Jetty web server.
 * <p>
 * Adapted from Drill's drill.exec.WebServer class. Would be good to create a
 * common base class later, but the goal for the initial project is to avoid
 * Drill code changes.
 *
 * @see <a href=
 *      "http://www.eclipse.org/jetty/documentation/current/embedding-jetty.html">
 *      Jetty Embedding documentation</a>
 */

public class WebServer implements AutoCloseable {
  private static final Log LOG = LogFactory.getLog(WebServer.class);
  private final Server jettyServer;
  private Dispatcher dispatcher;

  public WebServer(Dispatcher dispatcher) {
    this.dispatcher = dispatcher;
    Config config = DrillOnYarnConfig.config();
    if (config.getBoolean(DrillOnYarnConfig.HTTP_ENABLED)) {
      jettyServer = new Server();
    } else {
      jettyServer = null;
    }
  }

  /**
   * Start the web server including setup.
   *
   * @throws Exception in case of error during start
   */
  public void start() throws Exception {
    if (jettyServer == null) {
      return;
    }

    build();
    jettyServer.start();
  }

  private void build() throws Exception {
    Config config = DrillOnYarnConfig.config();
    buildConnector(config);
    buildServlets(config);
  }

  private void buildConnector(Config config) throws Exception {
    final ServerConnector serverConnector;
    if (config.getBoolean(DrillOnYarnConfig.HTTP_ENABLE_SSL)) {
      serverConnector = createHttpsConnector(config);
    } else {
      serverConnector = createHttpConnector(config);
    }
    jettyServer.addConnector(serverConnector);
  }

  /**
   * Build the web app with embedded servlets.
   * <p>
   * <b>ServletContextHandler</b>: is a Jetty-provided handler that add the
   * extra bits needed to set up the context that servlets expect. Think of it
   * as an adapter between the (simple) Jetty handler and the (more complex)
   * servlet API.
   *
   */
  private void buildServlets(Config config) {

    final ServletContextHandler servletContextHandler = new ServletContextHandler(
        null, "/");
    servletContextHandler.setErrorHandler(createErrorHandler());
    jettyServer.setHandler(servletContextHandler);

    // Servlet holder for the pages of the Drill AM web app. The web app is a
    // javax.ws application driven from annotations. The servlet holder "does
    // the right thing" to drive the application, which is rooted at "/".
    // The servlet container comes from Jersey, and manages the servlet
    // lifecycle.

    final ServletHolder servletHolder = new ServletHolder(
        new ServletContainer(new WebUiPageTree(dispatcher)));
    servletHolder.setInitOrder(1);
    servletContextHandler.addServlet(servletHolder, "/*");

    final ServletHolder restHolder = new ServletHolder(
        new ServletContainer(new AmRestApi(dispatcher)));
    restHolder.setInitOrder(2);
    servletContextHandler.addServlet(restHolder, "/rest/*");

    // Applying filters for CSRF protection.

    servletContextHandler.addFilter(CsrfTokenInjectFilter.class, "/*", EnumSet.of(DispatcherType.REQUEST));
    for (String path : new String[]{"/resize", "/stop", "/cancel"}) {
      servletContextHandler.addFilter(CsrfTokenValidateFilter.class, path, EnumSet.of(DispatcherType.REQUEST));
    }

    // Static resources (CSS, images, etc.)

    setupStaticResources(servletContextHandler);

    // Security, if requested.

    if (AMSecurityManagerImpl.isEnabled()) {
      servletContextHandler.setSecurityHandler(createSecurityHandler());
      servletContextHandler.setSessionHandler(createSessionHandler(config, servletContextHandler.getSecurityHandler()));
    }
  }

  private ErrorHandler createErrorHandler() {
    // Error handler to show detailed errors.
    // Should probably be turned off in production.
    final ErrorHandler errorHandler = new ErrorHandler();
    errorHandler.setShowStacks(true);
    errorHandler.setShowMessageInTitle(true);
    return errorHandler;
  }

  private void setupStaticResources(
      ServletContextHandler servletContextHandler) {

    // Access to static resources (JS pages, images, etc.)
    // The static resources themselves come from Drill exec sub-project
    // and the Drill-on-YARN project.
    //
    // We handle static content this way because we want to do it
    // in the context of a servlet app, so we need the Jetty "default servlet"
    // that handles static content. That servlet is designed to take its
    // properties
    // from the web.xml, file; but can also take them programmatically as done
    // here. (The Jetty manual suggests a simpler handler, but that is a
    // non-Servlet
    // version.)

    final ServletHolder staticHolder = new ServletHolder("static",
        DefaultServlet.class);
    staticHolder.setInitParameter("resourceBase",
        Resource.newClassPathResource("/rest/static").toString());
    staticHolder.setInitParameter("dirAllowed", "false");
    staticHolder.setInitParameter("pathInfoOnly", "true");
    servletContextHandler.addServlet(staticHolder, "/static/*");

    final ServletHolder amStaticHolder = new ServletHolder("am-static",
        DefaultServlet.class);
    amStaticHolder.setInitParameter("resourceBase",
        Resource.newClassPathResource("/drill-am/static").toString());
    amStaticHolder.setInitParameter("dirAllowed", "false");
    amStaticHolder.setInitParameter("pathInfoOnly", "true");
    servletContextHandler.addServlet(amStaticHolder, "/drill-am/static/*");
  }

  public static class AMUserPrincipal implements Principal {
    private final String userName;

    public AMUserPrincipal(String userName) {
      this.userName = userName;
    }

    @Override
    public String getName() {
      return userName;
    }
  }

  public static class AmLoginService implements LoginService {
    private final AMSecurityManager securityMgr;
    protected IdentityService identityService = new DefaultIdentityService();

    public AmLoginService(AMSecurityManager securityMgr) {
      this.securityMgr = securityMgr;
    }

    @Override
    public String getName() {
      return "drill-am";
    }

    @Override
    public UserIdentity login(String username, Object credentials, ServletRequest request) {
      if (!securityMgr.login(username, (String) credentials)) {
        return null;
      }
      return new DefaultUserIdentity(null, new AMUserPrincipal(username), new String[] { ADMIN_ROLE });
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
    public void logout(UserIdentity user) {
    }
  }

  /**
   * It creates handler with security constraint combinations for runtime efficiency
   * @see <a href="http://www.eclipse.org/jetty/documentation/current/embedded-examples.html">Eclipse Jetty Documentation</a>
   *
   * @return security handler with precomputed constraint combinations
   */
  private ConstraintSecurityHandler createSecurityHandler() {
    ConstraintSecurityHandler security = new ConstraintSecurityHandler();

    Set<String> knownRoles = ImmutableSet.of(ADMIN_ROLE);
    security.setConstraintMappings(Collections.emptyList(), knownRoles);

    security.setAuthenticator(new FormAuthenticator("/login", "/login", true));
    security
        .setLoginService(new AmLoginService(AMSecurityManagerImpl.instance()));

    return security;
  }

  /**
   * It creates a {@link SessionHandler} with a {@link org.eclipse.jetty.server.session.DefaultSessionCache}
   * which has {@link org.eclipse.jetty.server.session.NullSessionDataStore}.
   *
   * All sessions are stored in memory and on server restart are all destroyed.
   *
   * @param config Drill configs
   * @param securityHandler Set of initparameters that are used by the Authentication
   * @return session handler
   */
  private SessionHandler createSessionHandler(Config config,
      final SecurityHandler securityHandler) {
    SessionHandler sessionHandler = new SessionHandler();
    sessionHandler.setMaxInactiveInterval(
        config.getInt(DrillOnYarnConfig.HTTP_SESSION_MAX_IDLE_SECS));
    sessionHandler.addEventListener(new HttpSessionListener() {
      @Override
      public void sessionCreated(HttpSessionEvent se) {
        // No-op
      }

      @Override
      public void sessionDestroyed(HttpSessionEvent se) {
        final HttpSession session = se.getSession();
        if (session == null) {
          return;
        }

        final Object authCreds = session
            .getAttribute(SessionAuthentication.__J_AUTHENTICATED);
        if (authCreds != null) {
          final SessionAuthentication sessionAuth = (SessionAuthentication) authCreds;
          securityHandler.logout(sessionAuth);
          session.removeAttribute(SessionAuthentication.__J_AUTHENTICATED);
        }
      }
    });

    return sessionHandler;
  }

  /**
   * Create HTTP connector.
   *
   * @return Initialized {@link ServerConnector} instance for HTTP connections.
   */
  private ServerConnector createHttpConnector(Config config) {
    LOG.info("Setting up HTTP connector for web server");
    final ServerConnector httpConnector = new ServerConnector(jettyServer,
        new HttpConnectionFactory(baseHttpConfig()));
    httpConnector.setPort(config.getInt(DrillOnYarnConfig.HTTP_PORT));

    return httpConnector;
  }

  private HashMap<String,String> extractDrillYarnConfigs(Config config){
    HashMap<String,String> configMap = new HashMap<>();
    configMap.put("SSL_USE_HADOOP_CONF", String.valueOf(config.hasPath(DrillOnYarnConfig.SSL_USE_HADOOP_CONF)
            && config.getBoolean(DrillOnYarnConfig.SSL_USE_HADOOP_CONF)));
    configMap.put("SSL_USE_MAPR_CONFIG", String.valueOf(config.hasPath(DrillOnYarnConfig.SSL_USE_MAPR_CONFIG)
            && config.getBoolean(DrillOnYarnConfig.SSL_USE_MAPR_CONFIG)));
    return configMap;
  }

  /**
   * Create an HTTPS connector for given jetty server instance. If the admin has
   * specified keystore/truststore settings they will be used else a self-signed
   * certificate is generated and used.
   * <p>
   * With the use of {@link SslContextFactoryConfigurator} and configureNewDOYContextFactory
   * function this mechanism creates {@link ServerConnector}, in the same style as
   * org.apache.drill.exec.server.rest.WebServer#createHttpsConnector(int, int, int),
   * but with its own configs
   *
   * @return Initialized {@link ServerConnector} for HTTPS connections.
   * @throws Exception when unable to create HTTPS connector
   */
  private ServerConnector createHttpsConnector(Config config) throws Exception {
    LOG.info("Setting up HTTPS connector for web server");

//    TODO add valid domain to create self-signed certificate
//    String thisHostName = NetUtils.getHostname();
//    String names[] = thisHostName.split("/");
//    String domain = names[names.length - 1];

    SslContextFactory sslContextFactory = new SslContextFactoryConfigurator(null,"Drill AM")
            .configureNewDOYContextFactory(extractDrillYarnConfigs(config));

    final HttpConfiguration httpsConfig = baseHttpConfig();
    httpsConfig.addCustomizer(new SecureRequestCustomizer());

    // SSL Connector
    final ServerConnector sslConnector = new ServerConnector(jettyServer,
        new SslConnectionFactory(sslContextFactory,
            HttpVersion.HTTP_1_1.asString()),
        new HttpConnectionFactory(httpsConfig));
    sslConnector.setPort(config.getInt(DrillOnYarnConfig.HTTP_PORT));

    return sslConnector;
  }

  private HttpConfiguration baseHttpConfig() {
    HttpConfiguration httpConfig = new HttpConfiguration();
    httpConfig.setSendServerVersion(false);
    return httpConfig;
  }

  @Override
  public void close() throws Exception {
    if (jettyServer != null) {
      jettyServer.stop();
    }
  }
}
