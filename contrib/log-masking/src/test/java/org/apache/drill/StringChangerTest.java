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
import org.junit.Test;
import org.junit.jupiter.api.function.Executable;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;

import java.io.File;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


public class StringChangerTest {
  private static final ClassLoader classLoader = StringChangerTest.class.getClassLoader();

  @Test
  public void shouldDoNothingIfAbsentConfig() {
    //given
    final String missingConfigPath = "src/test/resources/missing-config.json";
    final Executable sut = () -> {
      StringChanger stringChanger = new StringChanger(missingConfigPath);
      String expected = "Expected string";
      String result = stringChanger.changeString(expected);
      assertEquals(expected, result);
    };

    //test
    assertDoesNotThrow(sut);
  }

  @Test
  public void shouldNotFailOnEmptyFile() {
    //given
    final String emptyConfigPath = classLoader.getResource("empty-config.json").getFile();
    final Executable sut = () -> new StringChanger(emptyConfigPath);

    //test
    assertDoesNotThrow(sut);
  }

  @Test
  public void throwExceptionIfFileHasBadJsonFormat() {
    //given
    final String brokenJsonConfigPath = classLoader.getResource("broken-json-config.json").getFile();
    final Executable sut = () -> new StringChanger(brokenJsonConfigPath);
    final Class expectedException = RuntimeException.class;
    final Class expectedCause = JsonMappingException.class;

    //test
    Throwable exceptionCause = assertThrows(expectedException, sut).getCause();
    assertEquals(expectedCause, exceptionCause.getClass());
  }

  @Test
  public void shouldApplyAllRules() {
    //given
    final String noRulesConfigPath = classLoader.getResource("no-rules-config.json").getFile();
    final MaskingRule[] ruleMocks = createRuleMocks(true);

    try (MockedConstruction<ObjectMapper> objectMapper = mockConstruction(ObjectMapper.class, (mock, context) -> {
      when(mock.readValue(any(File.class), eq(MaskingRule[].class)))
        .thenReturn(ruleMocks);
    })) {
      StringChanger cut = new StringChanger(noRulesConfigPath);

      //test
      cut.changeString("Some string");

      for (MaskingRule rule : ruleMocks) {
        verify(rule, times(1)).apply(anyString(), eq(null));
      }
    }
  }

  @Test
  public void shouldNotUseRulesWithNullSearchString() {
    //given
    final String noRulesConfigPath = classLoader.getResource("no-rules-config.json").getFile();
    final MaskingRule[] ruleMocks = createRuleMocks(false);

    try (MockedConstruction<ObjectMapper> objectMapper = mockConstruction(ObjectMapper.class, (mock, context) -> {
      when(mock.readValue(any(File.class), eq(MaskingRule[].class)))
        .thenReturn(ruleMocks);
    })) {
      StringChanger cut = new StringChanger(noRulesConfigPath);

      //test
      cut.changeString("Some string");

      for (MaskingRule rule : ruleMocks) {
        verify(rule, never()).apply(anyString(), anyString());
      }
    }
  }

  @Test
  public void shouldDoNothingIfNoRules() {
    //given
    final String noRulesConfigPath = classLoader.getResource("no-rules-config.json").getFile();
    final StringChanger cut = new StringChanger(noRulesConfigPath);
    final String stringToChange = "Some amazing message";

    //test
    String result = cut.changeString(stringToChange);
    assertEquals(stringToChange, result);
  }

  @Test
  public void shouldApplyUserNameToRule() {
    //given
    final MaskingRule[] ruleMocks = createRuleMocks(true);
    final String noRulesConfigPath = classLoader.getResource("no-rules-config.json").getFile();
    final String userName = "expectedUserName";

    try (MockedConstruction<ObjectMapper> objectMapper = mockConstruction(ObjectMapper.class, (mock, context) -> {
      when(mock.readValue(any(File.class), eq(MaskingRule[].class)))
        .thenReturn(ruleMocks);
    })) {
      StringChanger cut = new StringChanger(noRulesConfigPath);

      //test
      cut.changeString("Some string", userName);

      for (MaskingRule rule : ruleMocks) {
        verify(rule, times(1)).apply(anyString(), eq(userName));
      }
    }
  }

  private MaskingRule[] createRuleMocks(boolean activeRules) {
    MaskingRule[] ruleMocks = new MaskingRule[3];
    for (int count = 0; count < ruleMocks.length; count++) {
      MaskingRule ruleMock = mock(MaskingRule.class);
      when(ruleMock.apply(anyString(), Mockito.nullable(String.class))).thenReturn("stub");
      if (activeRules) {
        when(ruleMock.isActive()).thenReturn(true);
      }

      ruleMocks[count] = ruleMock;
    }
    return ruleMocks;
  }
}
