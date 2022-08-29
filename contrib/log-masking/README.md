# Masking sensitive data in logs

Drill has own implementations of logback encoder and layout to change(mask) data in logs.

`org.apache.drill.logback.MaskingPatternEncoder`
`org.apache.drill.logback.MaskingPatternLayout`

They are based on `ch.qos.logback.classic.encoder.PatternLayoutEncoder` and `ch.qos.logback.classic.PatternLayout`
accordingly, so they have all the same functions, plus — they can change the final log message according to a user 
defined rules.

Example of use `MaskingPatternEncoder`:
```xml
<configuration>
  <appender name="STDOUT" class="ch.qos.logback.core.ConsoleAppender">
    <encoder class="org.apache.drill.logback.MaskingPatternEncoder">
      <rulesConfig>${pathToJsonConfig}</rulesConfig>
      <pattern>%d{HH:mm:ss.SSS} [%thread] %-5level %logger{36} - %msg%n</pattern>
    </encoder>
  </appender>

  <root>
    <level value="error" />
    <appender-ref ref="STDOUT" />
  </root>
</configuration>
```

Example of `MaskingPatternLayout`:
```xml
<configuration>
  <appender name="STDOUT" class="ch.qos.logback.core.ConsoleAppender">
    <encoder class="ch.qos.logback.core.encoder.LayoutWrappingEncoder">
      <layout class="org.apache.drill.logback.MaskingPatternLayout">
        <rulesConfig>/home/maksym/dev/private-drill/distribution/target/apache-drill-1.20.2.0-eep-SNAPSHOT/apache-drill-1.20.2.0-eep-SNAPSHOT/conf/masking-rules.json</rulesConfig>
        <pattern>%d{HH:mm:ss.SSS} [%thread] %-5level %logger{36} - %msg%n</pattern>
      </layout>
    </encoder>
  </appender>

  <root>
    <level value="error" />
    <appender-ref ref="STDOUT" />
  </root>
</configuration>
```

They are both have a new option `rulesConfig` where user should provide a path to JSON config file with masking rules. If
to miss `rulesConfig` option then `MaskingPatternEncoder` and `MaskingPatternLayout` will work just like 
`PatternLayoutEncoder` and `PatternLayout`. 

Config file contains an array of rules in JSON format:
```json
[
  {
    "search": "([\\w\\d]+\\.)+(com)",
    "replace": "secret.domain.com",
    "description": "Mask domain names"
  },
  {
    "search": "MagicCompany",
    "replace": "TopSecretCompany",
    "description": "Mask company name"
  }
]
```   

Where:
1. `search` - defines a string or regex (*make sure that proper escaping is used*) pattern what is need to change/mask. 
              If it is empty, `null`, or omitted then the rule will be not used.
2. `replace` - defines a simple string to be changed to. Can be empty `"replace": ""` to just remove the search string
               and `""` it is the dafault value.
3. `descripton` - optional. Is intended for self-documentation purposes

Config file can be empty:
```json

```
or with empty array:
```json
[]
```

