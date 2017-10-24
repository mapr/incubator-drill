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
package org.apache.drill.exec.store.parquet;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.drill.common.logical.FormatPluginConfig;

import com.fasterxml.jackson.annotation.JsonTypeName;

@JsonTypeName("parquet") @JsonInclude(JsonInclude.Include.NON_DEFAULT)
public class ParquetFormatConfig implements FormatPluginConfig {

  public boolean autoCorrectCorruptDates = true;

  /**
   * @return true if auto correction of corrupt dates is enabled, false otherwise
   */
  @JsonIgnore
  public boolean areCorruptDatesAutoCorrected() {
    return autoCorrectCorruptDates;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    ParquetFormatConfig that = (ParquetFormatConfig) o;

    return autoCorrectCorruptDates == that.autoCorrectCorruptDates;

  }

  @Override
  public int hashCode() {
    return (autoCorrectCorruptDates ? 1231 : 1237);
  }
}