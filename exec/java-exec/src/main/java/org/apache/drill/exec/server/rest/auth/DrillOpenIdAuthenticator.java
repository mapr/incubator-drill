package org.apache.drill.exec.server.rest.auth;

import org.apache.drill.exec.server.rest.WebServerConstants;
import org.eclipse.jetty.security.LoginService;
import org.eclipse.jetty.security.ServerAuthException;
import org.eclipse.jetty.security.authentication.DeferredAuthentication;
import org.eclipse.jetty.security.authentication.LoginAuthenticator;
import org.eclipse.jetty.security.authentication.SessionAuthentication;
import org.eclipse.jetty.security.openid.OpenIdAuthenticator;
import org.eclipse.jetty.security.openid.OpenIdCredentials;
import org.eclipse.jetty.server.Authentication;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Response;
import org.eclipse.jetty.server.UserIdentity;
import org.eclipse.jetty.util.StringUtil;
import org.eclipse.jetty.util.URIUtil;
import org.eclipse.jetty.util.UrlEncoded;
import org.eclipse.jetty.util.security.Constraint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;
import java.io.IOException;
import java.io.Serializable;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

import static org.eclipse.jetty.security.openid.OpenIdAuthenticator.ERROR_PAGE;
import static org.eclipse.jetty.security.openid.OpenIdAuthenticator.RESPONSE;

public class DrillOpenIdAuthenticator extends LoginAuthenticator {
  private static final Logger logger = LoggerFactory.getLogger(DrillOpenIdAuthenticator.class);
  private static final String CSRF_MAP = "org.eclipse.jetty.security.openid.csrf_map";

  @Deprecated
  public static final String CSRF_TOKEN = "org.eclipse.jetty.security.openid.csrf_token";
  public static final String AUTH_URI = "/openid";
  public static final String REDIRECT_URI = AUTH_URI + "/callback";

  private final SecureRandom _secureRandom = new SecureRandom();
  private DrillOpenIdConfiguration _configuration;
  private String _logoutRedirectPath;
  private String _errorPage;
  private String _errorPath;
  private String _errorQuery;
  private String _redirectPath;
  private String _authPath;

  public DrillOpenIdAuthenticator(DrillOpenIdConfiguration configuration, String authenticationUri,
      String redirectUri, String errorPage, String logoutRedirectPath) {
    this._configuration = configuration;
    setRedirectPath(redirectUri);
    setAuthPath(authenticationUri);
    if (errorPage != null) {
      setErrorPage(errorPage);
    }
    if (logoutRedirectPath != null) {
      setLogoutRedirectPath(logoutRedirectPath);
    }
  }

  @Override
  public void setConfiguration(AuthConfiguration configuration) {
    super.setConfiguration(configuration);

    String error = configuration.getInitParameter(ERROR_PAGE);
    if (error != null) {
      setErrorPage(error);
    }

    if (_configuration != null) {
      return;
    }

    LoginService loginService = configuration.getLoginService();
    if (!(loginService instanceof DrillOpenIdLoginService)) {
      throw new IllegalArgumentException("invalid LoginService");
    }
    this._configuration = ((DrillOpenIdLoginService) loginService).getConfiguration();
  }

  @Override
  public String getAuthMethod() {
    return Constraint.__OPENID_AUTH;
  }

  private void setErrorPage(String path) {
    if (path == null || path.trim().length() == 0) {
      _errorPath = null;
      _errorPage = null;
    } else {
      if (!path.startsWith("/")) {
        logger.warn("error-page must start with /");
        path = "/" + path;
      }
      _errorPage = path;
      _errorPath = path;
      _errorQuery = "";

      int queryIndex = _errorPath.indexOf('?');
      if (queryIndex > 0) {
        _errorPath = _errorPage.substring(0, queryIndex);
        _errorQuery = _errorPage.substring(queryIndex + 1);
      }
    }
  }

  public void setLogoutRedirectPath(String logoutRedirectPath) {
    if (logoutRedirectPath == null) {
      logger.warn("redirect path must not be null, defaulting to /");
      logoutRedirectPath = "/";
    } else if (!logoutRedirectPath.startsWith("/")) {
      logger.warn("redirect path must start with /");
      logoutRedirectPath = "/" + logoutRedirectPath;
    }

    _logoutRedirectPath = logoutRedirectPath;
  }

  @Override
  public UserIdentity login(String username, Object credentials, ServletRequest request) {
    if (logger.isDebugEnabled()) {
      logger.debug("login {} {} {}", username, credentials, request);
    }

    UserIdentity user = super.login(username, credentials, request);
    if (user != null) {
      HttpSession session = ((HttpServletRequest) request).getSession();
      Authentication cached = new SessionAuthentication(getAuthMethod(), user, credentials);
      synchronized (session) {
        session.setAttribute(SessionAuthentication.__J_AUTHENTICATED, cached);
        session.setAttribute(OpenIdAuthenticator.CLAIMS,
            ((OpenIdCredentials) credentials).getClaims());
        session.setAttribute(RESPONSE, ((OpenIdCredentials) credentials).getResponse());
      }
    }
    return user;
  }

  @Override
  public void logout(ServletRequest request) {
    attemptLogoutRedirect(request);
    logoutWithoutRedirect(request);
  }

  private void logoutWithoutRedirect(ServletRequest request) {
    super.logout(request);
    HttpServletRequest httpRequest = (HttpServletRequest) request;
    HttpSession session = httpRequest.getSession(false);

    if (session == null) {
      return;
    }

    synchronized (session) {
      session.removeAttribute(SessionAuthentication.__J_AUTHENTICATED);
      session.removeAttribute(OpenIdAuthenticator.CLAIMS);
      session.removeAttribute(RESPONSE);
    }
  }

  private void attemptLogoutRedirect(ServletRequest request) {
    try {
      Request baseRequest = Objects.requireNonNull(Request.getBaseRequest(request));
      Response baseResponse = baseRequest.getResponse();
      String endSessionEndpoint = _configuration.getEndSessionEndpoint();
      String redirectUri = null;
      if (_logoutRedirectPath != null) {
        StringBuilder sb = URIUtil.newURIBuilder(request.getScheme(), request.getServerName(),
            request.getServerPort());
        sb.append(baseRequest.getContextPath());
        sb.append(_logoutRedirectPath);
        redirectUri = sb.toString();
      }

      HttpSession session = baseRequest.getSession(false);
      if (endSessionEndpoint == null || session == null) {
        if (redirectUri != null) {
          baseResponse.sendRedirect(redirectUri, true);
        }
        return;
      }

      Object openIdResponse = session.getAttribute(OpenIdAuthenticator.RESPONSE);
      if (!(openIdResponse instanceof Map)) {
        if (redirectUri != null) {
          baseResponse.sendRedirect(redirectUri, true);
        }
        return;
      }

      @SuppressWarnings("rawtypes")
      String idToken = (String) ((Map) openIdResponse).get("id_token");
      baseResponse.sendRedirect(endSessionEndpoint +
              "?id_token_hint=" + UrlEncoded.encodeString(idToken, StandardCharsets.UTF_8) +
              ((redirectUri == null) ? "" :
                  "&post_logout_redirect_uri=" + UrlEncoded.encodeString(redirectUri,
                      StandardCharsets.UTF_8)),
          true);
    } catch (Throwable t) {
      logger.warn("failed to redirect to end_session_endpoint", t);
    }
  }

  @Override
  public Authentication validateRequest(ServletRequest req, ServletResponse res,
      boolean mandatory) throws ServerAuthException {
    final HttpServletRequest request = (HttpServletRequest) req;
    final HttpServletResponse response = (HttpServletResponse) res;
    final Request baseRequest = Objects.requireNonNull(Request.getBaseRequest(request));
    final Response baseResponse = baseRequest.getResponse();

    if (logger.isDebugEnabled()) {
      logger.debug("validateRequest({},{},{})", req, res, mandatory);
    }

    String uri = request.getRequestURI();
    if (uri == null) {
      uri = URIUtil.SLASH;
    }

    if (!mandatory && !isOpenIdRequest(uri) ||
        isErrorPage(URIUtil.addPaths(request.getServletPath(), request.getPathInfo())) && !DeferredAuthentication.isDeferred(response)) {
      return new DeferredAuthentication(this);
    }

    try {
      // Get the Session.
      HttpSession session = request.getSession();
      if (request.isRequestedSessionIdFromURL()) {
        sendError(request, response, "Session ID must be a cookie to support OpenID " +
            "authentication");
        return Authentication.SEND_FAILURE;
      }

      // Handle a request for authentication.
      if (isCallback(uri)) {
        String authCode = request.getParameter("code");
        if (authCode == null) {
          sendError(request, response, "auth failed: no code parameter");
          return Authentication.SEND_FAILURE;
        }

        String state = request.getParameter("state");
        if (state == null) {
          sendError(request, response, "auth failed: no state parameter");
          return Authentication.SEND_FAILURE;
        }

        // Verify anti-forgery state token.
        UriRedirectInfo uriRedirectInfo;
        synchronized (session) {
          uriRedirectInfo = removeAndClearCsrfMap(session, state);
        }
        if (uriRedirectInfo == null) {
          sendError(request, response, "auth failed: invalid state parameter");
          return Authentication.SEND_FAILURE;
        }

        // Attempt to login with the provided authCode.
        OpenIdCredentials credentials = new OpenIdCredentials(authCode, getRedirectUri(request));
        UserIdentity user = login(null, credentials, request);
        if (user == null) {
          sendError(request, response, null);
          return Authentication.SEND_FAILURE;
        }

        OpenIdAuthenticator.OpenIdAuthentication openIdAuth =
            new OpenIdAuthenticator.OpenIdAuthentication(getAuthMethod(), user);
        if (logger.isDebugEnabled()) {
          logger.debug("authenticated {}->{}", openIdAuth, uriRedirectInfo.getUri());
        }

        // Redirect to the original URI.
        response.setContentLength(0);
        baseResponse.sendRedirect(uriRedirectInfo.getUri(), true);
        return openIdAuth;
      }

      // Look for cached authentication in the Session.
      Authentication authentication =
          (Authentication) session.getAttribute(SessionAuthentication.__J_AUTHENTICATED);
      if (authentication != null) {
        // Has authentication been revoked?
        if (authentication instanceof Authentication.User && _loginService != null &&
            !_loginService.validate(((Authentication.User) authentication).getUserIdentity())) {
          if (logger.isDebugEnabled()) {
            logger.debug("auth revoked {}", authentication);
          }
          logout(request);
        } else {
          if (logger.isDebugEnabled()) {
            logger.debug("auth {}", authentication);
          }
          return authentication;
        }
      }

      // If we can't send challenge.
      if (DeferredAuthentication.isDeferred(response)) {
        if (logger.isDebugEnabled()) {
          logger.debug("auth deferred {}", session.getId());
        }
        return Authentication.UNAUTHENTICATED;
      }

      // Send the challenge.
      String challengeUri = getChallengeUri(baseRequest);
      if (logger.isDebugEnabled()) {
        logger.debug("challenge {}->{}", session.getId(), challengeUri);
      }
      baseResponse.sendRedirect(challengeUri, true);

      return Authentication.SEND_CONTINUE;
    } catch (IOException e) {
      throw new ServerAuthException(e);
    }
  }

  /**
   * Report an error case either by redirecting to the error page if it is defined, otherwise
   * sending a 403 response.
   * If the message parameter is not null, a query parameter with a key of
   * {@link OpenIdAuthenticator#ERROR_PARAMETER} and value of the error
   * message will be logged and added to the error redirect URI if the error page is defined.
   *
   * @param request  the request.
   * @param response the response.
   * @param message  the reason for the error or null.
   * @throws IOException if sending the error fails for any reason.
   */
  private void sendError(HttpServletRequest request, HttpServletResponse response,
      String message) throws IOException {
    final Request baseRequest = Request.getBaseRequest(request);
    final Response baseResponse = Objects.requireNonNull(baseRequest).getResponse();

    if (logger.isDebugEnabled()) {
      logger.debug("OpenId authentication FAILED: {}", message);
    }

    if (_errorPage == null) {
      if (logger.isDebugEnabled()) {
        logger.debug("auth failed 403");
      }
      if (response != null) {
        response.sendError(HttpServletResponse.SC_FORBIDDEN);
      }
    } else {
      if (logger.isDebugEnabled()) {
        logger.debug("auth failed {}", _errorPage);
      }

      String redirectUri = URIUtil.addPaths(request.getContextPath(), _errorPage);
      if (message != null) {
        String query =
            URIUtil.addQueries(OpenIdAuthenticator.ERROR_PARAMETER + "=" + UrlEncoded.encodeString(message), _errorQuery);
        redirectUri = URIUtil.addPathQuery(URIUtil.addPaths(request.getContextPath(), _errorPath), query);
      }

      baseResponse.sendRedirect(redirectUri, true);
    }
  }

  public boolean isCallback(String uri) {
    return pathHasSegment(uri, _redirectPath);
  }

  public boolean isErrorPage(String pathInContext) {
    return pathInContext != null && (pathInContext.equals(_errorPath));
  }

  public boolean isAuthPage(String uri) {
    return pathHasSegment(uri, _authPath);
  }

  public boolean isOpenIdRequest(String uri) {
    return isAuthPage(uri) || isCallback(uri);
  }

  protected boolean pathHasSegment(String uri, String segment) {
    int segmentBeginIndex = uri.indexOf(segment);

    if (segmentBeginIndex < 0) {
      return false;
    }
    int nextToSegmentIndex = segmentBeginIndex + segment.length();
    if (nextToSegmentIndex == uri.length()) {
      return true;
    }
    char c = uri.charAt(nextToSegmentIndex);
    return c == ';' || c == '#' || c == '/' || c == '?';
  }

  private String getRedirectUri(HttpServletRequest request) {
    final StringBuffer redirectUri = new StringBuffer(128);
    URIUtil.appendSchemeHostPort(redirectUri, request.getScheme(),
        request.getServerName(), request.getServerPort());
    redirectUri.append(request.getContextPath());
    redirectUri.append(_redirectPath);
    return redirectUri.toString();
  }

  protected String getChallengeUri(Request request) {
    HttpSession session = request.getSession();
    String antiForgeryToken;
    synchronized (session) {
      Map<String, UriRedirectInfo> csrfMap = ensureCsrfMap(session);
      antiForgeryToken = new BigInteger(130, _secureRandom).toString(32);
      csrfMap.put(antiForgeryToken, new UriRedirectInfo(request));
    }

    // any custom scopes requested from configuration
    StringBuilder scopes = new StringBuilder();
    for (String s : _configuration.getScopes()) {
      scopes.append(" ").append(s);
    }

    return _configuration.getAuthEndpoint() +
        "?client_id=" + UrlEncoded.encodeString(_configuration.getClientId(),
        StandardCharsets.UTF_8) +
        "&redirect_uri=" + UrlEncoded.encodeString(getRedirectUri(request),
        StandardCharsets.UTF_8) +
        "&scope=openid" + UrlEncoded.encodeString(scopes.toString(), StandardCharsets.UTF_8) +
        "&state=" + antiForgeryToken +
        "&response_type=code";
  }

  @Override
  public boolean secureResponse(ServletRequest req, ServletResponse res, boolean mandatory,
      Authentication.User validatedUser) {
    return req.isSecure();
  }

  private UriRedirectInfo removeAndClearCsrfMap(HttpSession session, String csrf) {
    @SuppressWarnings("unchecked")
    Map<String, UriRedirectInfo> csrfMap =
        (Map<String, UriRedirectInfo>) session.getAttribute(CSRF_MAP);
    if (csrfMap == null) {
      return null;
    }

    UriRedirectInfo uriRedirectInfo = csrfMap.get(csrf);
    csrfMap.clear();
    return uriRedirectInfo;
  }

  private Map<String, UriRedirectInfo> ensureCsrfMap(HttpSession session) {
    @SuppressWarnings("unchecked")
    Map<String, UriRedirectInfo> csrfMap =
        (Map<String, UriRedirectInfo>) session.getAttribute(CSRF_MAP);
    if (csrfMap == null) {
      csrfMap = new MRUMap(64);
      session.setAttribute(CSRF_MAP, csrfMap);
    }
    return csrfMap;
  }

  public void setRedirectPath(String redirectPath) {
    if (redirectPath == null) {
      logger.warn("redirect path must not be null, defaulting to " + REDIRECT_URI);
      redirectPath = REDIRECT_URI;
    } else if (!redirectPath.startsWith("/")) {
      logger.warn("redirect path must start with /");
      redirectPath = "/" + redirectPath;
    }
    _redirectPath = redirectPath;
  }

  public void setAuthPath(String authPath) {
    if (authPath == null) {
      logger.warn("auth path must not be null, defaulting to " + AUTH_URI);
      authPath = AUTH_URI;
    } else if (!authPath.startsWith("/")) {
      logger.warn("auth path must start with /");
      authPath = "/" + authPath;
    }
    _authPath = authPath;
  }

  private static class MRUMap extends LinkedHashMap<String, UriRedirectInfo> {
    private static final long serialVersionUID = 5375723072014233L;

    private final int _size;

    private MRUMap(int size) {
      _size = size;
    }

    @Override
    protected boolean removeEldestEntry(Map.Entry<String, UriRedirectInfo> eldest) {
      return size() > _size;
    }
  }

  private static class UriRedirectInfo implements Serializable {
    private static final long serialVersionUID = 139567755844461433L;
    private final String _uri;

    public UriRedirectInfo(Request request) {
      String redirect = request.getParameter(WebServerConstants.REDIRECT_QUERY_PARM);
      if (StringUtil.isEmpty(redirect)) {
        redirect = "/";
      }
      _uri = redirect;
    }

    public String getUri() {
      return _uri;
    }
  }
}
