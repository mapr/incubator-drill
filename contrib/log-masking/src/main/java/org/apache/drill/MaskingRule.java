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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.drill.shaded.guava.com.google.common.base.Strings;

import java.util.Objects;
import java.util.function.BiFunction;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

@JsonIgnoreProperties({"description"})
public class MaskingRule implements BiFunction<String, String, String> {
  private Pattern search;
  private String replace;
  private Pattern includeUserPattern;
  private Pattern excludeUserPattern;

  public MaskingRule(String search) {
    this(search, null, null, null);
  }
  public MaskingRule(String search, String replace) {
    this(search, replace, null, null);
  }

  public MaskingRule(String search, String replace, String includeUser) {
    this(search, replace, includeUser, null);
  }

  public MaskingRule(@JsonProperty("search") String search, @JsonProperty("replace") String replace,
                     @JsonProperty("includeUser") String includeUser, @JsonProperty("excludeUser") String excludeUser) {
    compileSearchPattern(search);
    setReplace(replace);
    compileIncludeUserPattern(includeUser);
    compileExcludeUserPattern(excludeUser);
  }

  @Override
  public String apply(String str, String username) {
    if (isActive() && userShouldBeMasked(username)) {
      return search.matcher(str).replaceAll(replace);
    } else {
      return str;
    }
  }

  private void compileSearchPattern(String search) {
    try {
      if (!Strings.isNullOrEmpty(search)) {
        this.search = Pattern.compile(search);
      }
    } catch (PatternSyntaxException patternException) {
      throw new IllegalArgumentException("Search string for MaskingRule should be a valid regular expression");
    }
  }

  private boolean userShouldBeMasked(String username) {
    if (Strings.isNullOrEmpty(username)) {
      return true;
    }

    if (excludeUserPattern != null && excludeUserPattern.matcher(username).matches()) {
      return false;
    }
    if (includeUserPattern != null) {
      return includeUserPattern.matcher(username).matches();
    }
    return true;
  }

  private void setReplace(String replace) {
    this.replace = replace == null ? "" : replace;
  }

  private void compileIncludeUserPattern(String includeUser) {
    try {
      if (!Strings.isNullOrEmpty(includeUser)) {
        this.includeUserPattern = Pattern.compile(includeUser);
      }
    } catch (PatternSyntaxException patternException) {
      throw new IllegalArgumentException("IncludeUser string for MaskingRule should be a valid regular expression");
    }
  }

  private void compileExcludeUserPattern(String excludeUser) {
    try {
      if (!Strings.isNullOrEmpty(excludeUser)) {
        this.excludeUserPattern = Pattern.compile(excludeUser);
      }
    } catch (PatternSyntaxException patternException) {
      throw new IllegalArgumentException("ExcludeUser string for MaskingRule should be a valid regular expression");
    }
  }

  public boolean isActive() {
    return Objects.nonNull(search);
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
    return Objects.equals(search, that.search) &&
      replace.equals(that.replace) &&
      Objects.equals(includeUserPattern, that.includeUserPattern) &&
      Objects.equals(excludeUserPattern, that.excludeUserPattern);
  }

  @Override
  public int hashCode() {
    return Objects.hash(search, replace, includeUserPattern, excludeUserPattern);
  }

}
