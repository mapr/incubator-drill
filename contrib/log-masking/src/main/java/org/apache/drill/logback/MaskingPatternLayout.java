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
package org.apache.drill.logback;

import ch.qos.logback.classic.PatternLayout;
import ch.qos.logback.classic.spi.ILoggingEvent;
import org.apache.drill.StringChanger;
import org.slf4j.MDC;

public class MaskingPatternLayout extends PatternLayout {
  private StringChanger stringChanger;

  public void setRulesConfig(String configFilePath) {
    if (configFilePath == null) {
      return;
    }
    stringChanger = new StringChanger(configFilePath);
  }

  @Override
  public String doLayout(ILoggingEvent event) {
    String formattedString = super.doLayout(event);
    return maskMessage(formattedString);
  }

  private String maskMessage(String message) {
    String userName = MDC.get("drill.userName");
    return stringChanger != null ? stringChanger.changeString(message, userName) : message;
  }
}
