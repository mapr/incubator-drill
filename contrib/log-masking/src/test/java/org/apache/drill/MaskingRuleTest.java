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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.api.function.ThrowingSupplier;

import java.io.File;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;


public class MaskingRuleTest {

  @Test
  public void canBeUsedToDeserializeJsonConfig() {
    //given
    final ObjectMapper objectMapper = new ObjectMapper();
    final File jsonConfig = new File("src/test/resources/test-config.json");
    final ThrowingSupplier<MaskingRule[]> sut = () -> objectMapper.readValue(jsonConfig, MaskingRule[].class);

    //test
    MaskingRule[] maskingRules = assertDoesNotThrow(sut);
    assertTrue(maskingRules.length == 5);
  }

  @Test
  public void workForMultilineString() {
    final MaskingRule cut = new MaskingRule("password", "secret");

    //given
    final String stringToChange = "1st line with password\n" +
      "2nd line with password\n" +
      "3d line with password";

    final String expectedString = "1st line with secret\n" +
      "2nd line with secret\n" +
      "3d line with secret";

    //test
    String result = cut.apply(stringToChange);
    assertEquals(expectedString, result);
  }

  @Test
  public void doNothingIfSearchStringIsNotFound() {
    final String replaceString = "***";
    final MaskingRule cut = new MaskingRule("password", replaceString);

    //given
    final String stringToChange = "Name Surname";

    //test
    String result = cut.apply(stringToChange);
    assertEquals(stringToChange, result);
  }

  @Test
  public void workWithRegexSearchString() {
    final String replaceString = "secret";
    final MaskingRule cut = new MaskingRule("[a-zA-Z.]@[a-zA-Z.]", replaceString);

    //given
    final String stringToChange = "User email: name.surname@mail.com";

    //test
    String result = cut.apply(stringToChange);
    assertFalse(result.contains("password"));
    assertTrue(result.contains(replaceString));
  }

  @Test
  public void maskAllSearchOccurrence() {
    final String replaceString = "secret";
    final MaskingRule cut = new MaskingRule("[pasword]{8}", replaceString);

    //given
    final String stringToChange = "Many password, password, password";

    //test
    String result = cut.apply(stringToChange);
    assertFalse(result.contains("password"));
    assertTrue(result.contains(replaceString));
  }

  @Test
  public void throwExceptionOnInvalidRegexSearchString() {
    final Executable cut = () -> new MaskingRule("[\\]", "something");

    //given
    final Class expectedException = IllegalArgumentException.class;

    //test
    assertThrows(expectedException, cut);
  }

  @Test
  public void removeSearchStringOnEmptyReplaceString() {
    //given
    final String search = "user.email@mail.com";
    final MaskingRule cut = new MaskingRule(search, "");
    final String stringToChange = "User email: user.email@mail.com";
    final String expectedString = "User email: ";

    //test
    String result = cut.apply(stringToChange);
    assertEquals(expectedString, result);
  }

  @Test
  public void shouldNotChangeStringOnEmptySearch() {
    final String replaceString = "magic";
    final MaskingRule cut = new MaskingRule("", replaceString);

    //given
    final String stringToChange = "This string shouldn't be changed";

    //test
    String result = cut.apply(stringToChange);
    assertEquals(stringToChange, result);
  }
}
