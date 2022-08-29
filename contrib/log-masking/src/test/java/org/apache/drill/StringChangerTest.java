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

import java.io.File;
import java.io.FileNotFoundException;

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

  @Test
  public void failOnInvalidFilePath() {
    //given
    final String invalidFilePath = "folder1/.|?,{([";
    final Executable sut = () -> new StringChanger(invalidFilePath);


    //test
    assertThrows(Exception.class, sut);
  }

  @Test
  public void failOnMissingFile() {
    //given
    final Executable sut = () -> new StringChanger("src/test/resources/missing-config.json");
    final Class expectedException = RuntimeException.class;
    final Class expectedCause = FileNotFoundException.class;

    //test
    Throwable exceptionCause = assertThrows(expectedException, sut).getCause();
    assertEquals(expectedCause, exceptionCause.getClass());
  }

  @Test
  public void doNotFailOnEmptyFile() {
    //given
    final Executable sut = () -> new StringChanger("src/test/resources/empty-config.json");

    //test
    assertDoesNotThrow(sut);
  }

  @Test
  public void throwExceptionIfFileHasBadJsonFormat() {
    //given
    final Executable sut = () -> new StringChanger("src/test/resources/broken-json-config.json");
    final Class expectedException = RuntimeException.class;
    final Class expectedCause = JsonMappingException.class;

    //test
    Throwable exceptionCause = assertThrows(expectedException, sut).getCause();
    assertEquals(expectedCause, exceptionCause.getClass());
  }

  @Test
  public void applyAllRules() {
    //given
    MaskingRule[] ruleMocks = createRuleMocks(true);

    try (MockedConstruction<ObjectMapper> objectMapper = mockConstruction(ObjectMapper.class, (mock, context) -> {
      when(mock.readValue(any(File.class), eq(MaskingRule[].class)))
        .thenReturn(ruleMocks);
    })) {
      StringChanger cut = new StringChanger("src/test/resources/no-rules-config.json");

      //test
      cut.changeString("Some string");

      for (MaskingRule rule : ruleMocks) {
        verify(rule, times(1)).apply(anyString());
      }
    }
  }

  @Test
  public void doNotUseRulesWithNullSearchString() {
    //given
    MaskingRule[] ruleMocks = createRuleMocks(false);

    try (MockedConstruction<ObjectMapper> objectMapper = mockConstruction(ObjectMapper.class, (mock, context) -> {
      when(mock.readValue(any(File.class), eq(MaskingRule[].class)))
        .thenReturn(ruleMocks);
    })) {
      StringChanger cut = new StringChanger("src/test/resources/no-rules-config.json");

      //test
      cut.changeString("Some string");

      for (MaskingRule rule : ruleMocks) {
        verify(rule, never()).apply(anyString());
      }
    }
  }

  private MaskingRule[] createRuleMocks(boolean hasSearchString) {
    MaskingRule[] ruleMocks = new MaskingRule[3];
    for (int count = 0; count < ruleMocks.length; count++) {
      MaskingRule ruleMock = mock(MaskingRule.class);
      when(ruleMock.apply(anyString())).thenReturn("stub");
      if (hasSearchString) {
        when(ruleMock.getSearch()).thenReturn("stub");
      }

      ruleMocks[count] = ruleMock;
    }
    return ruleMocks;
  }

  @Test
  public void doNothingIfNoRules() {
    //given
    final StringChanger cut = new StringChanger("src/test/resources/no-rules-config.json");
    final String stringToChange = "Some amazing message";

    //test
    String result = cut.changeString(stringToChange);
    assertEquals(stringToChange, result);
  }
}
