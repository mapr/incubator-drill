package org.apache.drill.logback;

import ch.qos.logback.classic.spi.ILoggingEvent;
import org.apache.drill.StringChanger;
import org.junit.Test;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.slf4j.MDC;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class MaskingPatternLayoutTest {
  @Test
  public void shouldUseMDCForUserName() {
    final MockedConstruction<StringChanger> stringChangerConstruction = mockConstruction(StringChanger.class);
    final ILoggingEvent eventMock = mock(ILoggingEvent.class);

    MaskingPatternLayout cut = new MaskingPatternLayout();
    cut.setRulesConfig("/some/path");

    //given
    final String mdcKey = "drill.userName";
    final String givenUserName = "testUser";
    final String expectedUserName = new String(givenUserName);

    MockedStatic<MDC> mdcMocked = Mockito.mockStatic(MDC.class);
    mdcMocked.when(() -> MDC.get(mdcKey))
      .thenReturn(givenUserName);

    //test
    cut.doLayout(eventMock);


    //verify
    List<StringChanger> stringChangerMocks = stringChangerConstruction.constructed();

    assertEquals(1, stringChangerMocks.size(), MaskingPatternLayout.class.getName() + " should construct only one " + StringChanger.class.getName());
    verify(stringChangerMocks.get(0), times(1))
      .changeString(anyString(), Mockito.eq(expectedUserName));
    mdcMocked.verify(() -> MDC.get(mdcKey), times(1));

    stringChangerConstruction.close();
    mdcMocked.close();
  }
}
