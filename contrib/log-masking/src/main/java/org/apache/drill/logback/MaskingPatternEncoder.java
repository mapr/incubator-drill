package org.apache.drill.logback;

import ch.qos.logback.classic.encoder.PatternLayoutEncoder;

public class MaskingPatternEncoder extends PatternLayoutEncoder {
  private String configFilePath;

  public void setRulesConfig(String configFilePath) {
    this.configFilePath = configFilePath;
  }

  @Override
  public void start() {
    super.start();
    MaskingPatternLayout maskingPatternLayout = new MaskingPatternLayout();
    maskingPatternLayout.setContext(context);
    maskingPatternLayout.setPattern(getPattern());
    maskingPatternLayout.setRulesConfig(configFilePath);
    maskingPatternLayout.setOutputPatternAsHeader(outputPatternAsHeader);
    maskingPatternLayout.start();
    this.layout = maskingPatternLayout;
  }
}
