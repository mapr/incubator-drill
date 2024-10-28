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
package org.apache.drill.exec.rpc.security;

import org.apache.drill.common.AutoCloseables;
import org.apache.drill.common.map.CaseInsensitiveMap;
import org.apache.drill.exec.rpc.security.kerberos.KerberosFactory;
import org.apache.drill.exec.rpc.security.plain.PlainFactory;

import javax.security.sasl.SaslException;
import java.util.Map;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;
import java.util.Set;

public class ClientAuthenticatorProvider implements AuthenticatorProvider {
  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(ClientAuthenticatorProvider.class);

  public static final String CUSTOM_FACTORIES_PROPERTY_NAME = "drill.customAuthFactories";
  private static final String customFactories = System.getProperty(CUSTOM_FACTORIES_PROPERTY_NAME);

  private final Map<String, AuthenticatorFactory> authFactories =
      CaseInsensitiveMap.newHashMapWithExpectedSize(5);
  static final ClientAuthenticatorProvider INSTANCE = new ClientAuthenticatorProvider();

  public static ClientAuthenticatorProvider getInstance() {
    return INSTANCE;
  }

  private ClientAuthenticatorProvider() {
    if (customFactories == null) {
      loadAuthFactoriesOverSPI();
    } else {
      loadAuthFactoriesByClassName();
    }
    if (logger.isDebugEnabled()) {
      logger.debug("Configured mechanisms: {}", authFactories.keySet());
    }
  }

  /**
   * Load all auth factories over SPI, including the default Drill.
   *
   * @return found and loaded auth factories
   */
  private void loadAuthFactoriesOverSPI() {
    try {
      ServiceLoader
          .load(AuthenticatorFactory.class)
          .forEach(authFactory -> addAuthFactory(authFactory));
    } catch (ServiceConfigurationError error) {
      logger.error("Failed to load auth factories", error);
    }
  }

  /**
   * Load auth factories by class name.
   * <p>
   * Load the default auth factories bundled with Drill and factories provided in the system
   * property {@link #CUSTOM_FACTORIES_PROPERTY_NAME}.
   *
   * @return found and loaded auth factories
   */
  private void loadAuthFactoriesByClassName() {
    // factories provided by Drill
    final KerberosFactory kerberosFactory = new KerberosFactory();
    addAuthFactory(kerberosFactory);
    final PlainFactory plainFactory = new PlainFactory();
    addAuthFactory(plainFactory);

    // then, custom factories
    if (customFactories != null) {
      final String[] factories = customFactories.split(",");
      for (final String factory : factories) {
        try {
          final Class<?> clazz = Class.forName(factory);
          if (AuthenticatorFactory.class.isAssignableFrom(clazz)) {
            final AuthenticatorFactory instance = (AuthenticatorFactory) clazz.newInstance();
            addAuthFactory(instance);
          }
        } catch (final ReflectiveOperationException e) {
          logger.error("Failed to create auth factory {}", factory, e);
        }
      }
    }
  }

  /**
   * Add {@link AuthenticatorFactory} implementation to this {@link ClientAuthenticatorProvider}.
   * Overrides previously added implementations with the same mechanism name.
   *
   * @param authFactory - implementation to add to this provider.
   */
  private void addAuthFactory(AuthenticatorFactory authFactory) {
    AuthenticatorFactory previousAuthFactory =
        authFactories.put(authFactory.getSimpleName(), authFactory);
    if (previousAuthFactory != null) {
      logger.warn("Found multiple implementations of {} authentication mechanism: {}, {}",
          authFactory.getSimpleName(),
          authFactory.getClass().getName(),
          previousAuthFactory.getClass().getName());
      logger.warn("Preserved {} implementation for {} mechanism",
          authFactory.getSimpleName(),
          authFactory.getClass().getName());
    }
  }

  @Override
  public AuthenticatorFactory getAuthenticatorFactory(final String name) throws SaslException {
    final AuthenticatorFactory mechanism = authFactories.get(name);
    if (mechanism == null) {
      throw new SaslException(String.format("Unknown mechanism: '%s' Configured mechanisms: %s",
          name, authFactories.keySet()));
    }
    return mechanism;
  }

  @Override
  public Set<String> getAllFactoryNames() {
    return authFactories.keySet();
  }

  @Override
  public boolean containsFactory(final String name) {
    return authFactories.containsKey(name);
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close(authFactories.values());
    authFactories.clear();
  }
}
