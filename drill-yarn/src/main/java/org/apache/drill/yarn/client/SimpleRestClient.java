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
package org.apache.drill.yarn.client;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;

import com.typesafe.config.Config;
import org.apache.drill.yarn.core.DrillOnYarnConfig;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.BasicHttpClientConnectionManager;
import org.apache.http.ssl.SSLContexts;

import javax.net.ssl.SSLContext;

public class SimpleRestClient {
  public String send(String baseUrl, String resource, boolean isPost)
      throws ClientException {
    String url = baseUrl;
    if (!url.endsWith("/")) {
      url += "/";
    }
    url += resource;
    try {
      SSLContext sslContext = SSLContexts.createDefault();
      Config config = DrillOnYarnConfig.config();
      if (config.hasPath(DrillOnYarnConfig.DISABLE_CERT_VERIFICATION) && config.getBoolean(DrillOnYarnConfig.DISABLE_CERT_VERIFICATION)) {
        sslContext = SSLContexts.custom().loadTrustMaterial(null, (cert, authType) -> true).build();
      }

      SSLConnectionSocketFactory sslsf = new SSLConnectionSocketFactory(sslContext);
      if (config.hasPath(DrillOnYarnConfig.DISABLE_HOST_VERIFICATION) && config.getBoolean(DrillOnYarnConfig.DISABLE_HOST_VERIFICATION) ||
              config.hasPath(DrillOnYarnConfig.DISABLE_CERT_VERIFICATION) && config.getBoolean(DrillOnYarnConfig.DISABLE_CERT_VERIFICATION)) {
        sslsf = new SSLConnectionSocketFactory(sslContext, NoopHostnameVerifier.INSTANCE);
      }

      Registry<ConnectionSocketFactory> socketFactoryRegistry =
              RegistryBuilder.<ConnectionSocketFactory>create()
                      .register("https", sslsf)
                      .register("http", new PlainConnectionSocketFactory())
                      .build();

      BasicHttpClientConnectionManager connectionManager =
              new BasicHttpClientConnectionManager(socketFactoryRegistry);

      try (CloseableHttpClient httpClient = HttpClients.custom().setSSLSocketFactory(sslsf)
              .setConnectionManager(connectionManager).build()) {
        HttpRequestBase request;
        if (isPost) {
          request = new HttpPost(url);
        } else {
          request = new HttpGet(url);
        }

        try (CloseableHttpResponse response = httpClient.execute(request)){
          BufferedReader rd = new BufferedReader(
                  new InputStreamReader(response.getEntity().getContent()));
          StringBuilder buf = new StringBuilder();
          String line = null;
          while ((line = rd.readLine()) != null) {
            buf.append(line);
          }
          return buf.toString().trim();
        }
      }
    } catch (ClientProtocolException | KeyStoreException | NoSuchAlgorithmException
            | KeyManagementException | IllegalStateException e) {
      throw new ClientException("Internal REST error", e);
    } catch (IOException e) {
      throw new ClientException("REST request failed: " + url, e);
    }
  }
}
