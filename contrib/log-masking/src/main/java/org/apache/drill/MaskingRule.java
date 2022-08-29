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

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.drill.shaded.guava.com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

public class MaskingRule implements Function<String, String> {
  private static final Logger logger = LoggerFactory.getLogger(MaskingRule.class);
  private static final String DEFAULT_RULE_DESCRIPTION = "Empty rule description";
  private String description;
  private Pattern search;
  private String replace;

  public MaskingRule(String search, String replace) {
    this(search, replace, null);
  }

  public MaskingRule(@JsonProperty("search") String search, @JsonProperty("replace") String replace,
                     @JsonProperty("description") String description) {
    this.setSearch(search);
    this.setReplace(replace);
    this.setDescription(description);
  }

  @Override
  public String apply(String str) {
    if (search == null) {
      logger.debug("Masking rule '{}' wasn't applied since search string is empty or null.", description);
      return str;
    }
    return search.matcher(str).replaceAll(replace);
  }

  private void setSearch(String search) {
    if (Strings.isNullOrEmpty(search)) {
      this.search = null;
      return;
    }
    try {
      this.search = Pattern.compile(search);
    } catch (PatternSyntaxException patternException) {
      throw new IllegalArgumentException("Search string for MaskingRule should be a valid regular expression");
    }
  }

  private void setReplace(String replace) {
    this.replace = replace == null ? "" : replace;
  }

  private void setDescription(String description) {
    this.description = Strings.isNullOrEmpty(description) ? DEFAULT_RULE_DESCRIPTION : description;
  }

  public String getSearch() {
    return search == null ? null : search.pattern();
  }

  public String getReplace() {
    return replace;
  }

  public String getDescription() {
    return description;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    MaskingRule that = (MaskingRule) o;
    return description.equals(that.description) &&
      Objects.equals(search, that.search) &&
      replace.equals(that.replace);
  }

  @Override
  public int hashCode() {
    return Objects.hash(description, search, replace);
  }
}
