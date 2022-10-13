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
package org.apache.drill;

import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class StringChanger {
  private static final Logger logger = LoggerFactory.getLogger(StringChanger.class);
  private List<MaskingRule> maskingRules = new ArrayList<>();

  public StringChanger(String jsonFilePath) {
    File jsonFile = new File(jsonFilePath);

    if (!jsonFile.exists()) {
      logger.error("Configuration file {} is not found. No one masking rule will be applied.", jsonFile.getAbsolutePath());
      return;
    }

    if (jsonFile.length() == 0) {
      logger.info("Configuration file {} is empty. No one masking rule will be applied.", jsonFile.getAbsolutePath());
      return;
    }

    ObjectMapper objectMapper = new ObjectMapper();
    try {
      MaskingRule[] maskingRules = objectMapper.readValue(jsonFile, MaskingRule[].class);

      this.maskingRules = Stream.of(maskingRules)
        .filter(maskingRule -> maskingRule.getSearch() != null)
        .collect(Collectors.toList());

    } catch (IOException exception) {
      if (exception instanceof JsonMappingException) {
        logger.error("Configuration file {} has broken format", jsonFile.getAbsolutePath());
      }
      throw new RuntimeException(exception);
    }
  }

  public String changeString(String strToChange) {
    for (MaskingRule maskingRule : maskingRules) {
      strToChange = maskingRule.apply(strToChange);
    }
    return strToChange;
  }
}
