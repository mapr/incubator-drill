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
  private static final ClassLoader classLoader = MaskingRuleTest.class.getClassLoader();

  @Test
  public void canBeUsedToDeserializeJsonConfig() {
    //given
    final ObjectMapper objectMapper = new ObjectMapper();
    final File jsonConfig = new File(classLoader.getResource("test-config.json").getFile());
    final ThrowingSupplier<MaskingRule[]> sut = () -> objectMapper.readValue(jsonConfig, MaskingRule[].class);

    //test
    MaskingRule[] maskingRules = assertDoesNotThrow(sut);
    assertTrue(maskingRules.length == 10);
  }

  @Test
  public void shouldChangeMultilineString() {
    final MaskingRule cut = new MaskingRule("password", "secret");

    //given
    final String stringToChange = "1st line with password\n" +
      "2nd line with password\n" +
      "3d line with password";

    final String expectedString = "1st line with secret\n" +
      "2nd line with secret\n" +
      "3d line with secret";

    //test
    String result = cut.apply(stringToChange, null);
    assertEquals(expectedString, result);
  }

  @Test
  public void doNothingIfSearchStringIsNotFound() {
    final String replaceString = "***";
    final MaskingRule cut = new MaskingRule("password", replaceString);

    //given
    final String stringToChange = "Name Surname";

    //test
    String result = cut.apply(stringToChange, null);
    assertEquals(stringToChange, result);
  }

  @Test
  public void shouldWorkWithRegexSearchString() {
    final String replaceString = "secret";
    final MaskingRule cut = new MaskingRule("[a-zA-Z.]@[a-zA-Z.]", replaceString);

    //given
    final String stringToChange = "User email: name.surname@mail.com";

    //test
    String result = cut.apply(stringToChange, null);
    assertFalse(result.contains("password"));
    assertTrue(result.contains(replaceString));
  }

  @Test
  public void shouldMaskAllSearchOccurrence() {
    final String replaceString = "secret";
    final MaskingRule cut = new MaskingRule("[pasword]{8}", replaceString);

    //given
    final String stringToChange = "Many password, password, password";

    //test
    String result = cut.apply(stringToChange, null);
    assertFalse(result.contains("password"));
    assertTrue(result.contains(replaceString));
  }

  @Test
  public void throwExceptionOnInvalidRegexSearchString() {
    final String invalidRegex = "[\\]";
    final Executable cut = () -> new MaskingRule(invalidRegex);

    //given
    final Class expectedException = IllegalArgumentException.class;

    //test
    assertThrows(expectedException, cut);
  }

  @Test
  public void shouldRemoveSearchStringOnEmptyReplaceString() {
    //given
    final String search = "user.email@mail.com";
    final MaskingRule cut = new MaskingRule(search, "");
    final String stringToChange = "User email: user.email@mail.com";
    final String expectedString = "User email: ";

    //test
    String result = cut.apply(stringToChange, null);
    assertEquals(expectedString, result);
  }

  @Test
  public void shouldNotChangeStringOnEmptySearch() {
    final String replaceString = "magic";
    final MaskingRule cut = new MaskingRule("", replaceString);

    //given
    final String stringToChange = "This string shouldn't be changed";

    //test
    String result = cut.apply(stringToChange, null);
    assertEquals(stringToChange, result);
  }

  @Test
  public void shouldChangeChangeStringForMatchedUser() {
    final String search = "user.email@mail.com";
    final String replaceString = "***secret email***";
    final String userName = "secretUser";
    final MaskingRule cut = new MaskingRule(search, replaceString, userName);

    //given
    final String stringToChange = "User email: user.email@mail.com";
    final String expectedString = "User email: " + replaceString;
    final String userToMatch = new String(userName);

    //test
    String result = cut.apply(stringToChange, userToMatch);
    assertEquals(expectedString, result);
  }

  @Test
  public void shouldNotChangeChangeStringForUnmatchedUser() {
    final String search = "user.email@mail.com";
    final String replaceString = "***secret email***";
    final String userName = "secretUser";
    final MaskingRule cut = new MaskingRule(search, replaceString, userName);

    //given
    final String stringToChange = "User email: user.email@mail.com";
    final String otherUserName = "otherSecretUser";

    //test
    String result = cut.apply(stringToChange, otherUserName);
    assertEquals(stringToChange, result);
  }

  @Test
  public void shouldApplyFilterOnIncludeUserWithRegex() {
    final String search = "user.email@mail.com";
    final String replaceString = "***secret email***";
    final String userName = ".*User";
    final MaskingRule cut = new MaskingRule(search, replaceString, userName);

    //given
    final String stringToChange = "User email: user.email@mail.com";
    final String expectedString = "User email: " + replaceString;
    final String userToMatch = new String(userName);

    //test
    String result = cut.apply(stringToChange, userToMatch);
    assertEquals(expectedString, result);
  }

  @Test
  public void shouldNotApplyFilterOnExcludedUserWithRegex() {
    final String search = "user.email@mail.com";
    final String replaceString = "***secret email***";
    final String userNamePatternToExclude = "mapr.*";
    final MaskingRule cut = new MaskingRule(search, replaceString, null, userNamePatternToExclude);

    //given
    final String stringToChange = "User email: user.email@mail.com";
    final String userToTest = "maprUser";

    //test
    String result = cut.apply(stringToChange, userToTest);
    assertEquals(stringToChange, result);
  }

  @Test
  public void shouldNotApplyFilterOnExcludedUserWithRegexDespiteIncludedUser() {
    final String search = "user.email@mail.com";
    final String replaceString = "***secret email***";
    final String userNamePatternToInclude = "maprUser";
    final String userNamePatternToExclude = "mapr.*";
    final MaskingRule cut = new MaskingRule(search, replaceString, userNamePatternToInclude, userNamePatternToExclude);

    //given
    final String stringToChange = "User email: user.email@mail.com";
    final String userToTest = "maprUser";

    //test
    String result = cut.apply(stringToChange, userToTest);
    assertEquals(stringToChange, result);
  }

  @Test
  public void shouldThrowIllegalArgumentExceptionOnInvalidIncludeUserRegex() {
    //given
    final String invalidRegex = "**";
    final Class expectedException = IllegalArgumentException.class;
    final Executable sut = () -> new MaskingRule(null, null, invalidRegex, null);

    //test
    assertThrows(expectedException, sut);
  }

  @Test
  public void shouldThrowIllegalArgumentExceptionOnInvalidExcludeUserRegex() {
    //given
    final String invalidRegex = "**";
    final Class expectedException = IllegalArgumentException.class;
    final Executable sut = () -> new MaskingRule(null, null, null, invalidRegex);

    //test
    assertThrows(expectedException, sut);
  }
}
