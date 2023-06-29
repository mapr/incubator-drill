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
    <level value="error"/>
    <appender-ref ref="STDOUT"/>
  </root>
</configuration>
```

Example of `MaskingPatternLayout`:

```xml

<configuration>
  <appender name="STDOUT" class="ch.qos.logback.core.ConsoleAppender">
    <encoder class="ch.qos.logback.core.encoder.LayoutWrappingEncoder">
      <layout class="org.apache.drill.logback.MaskingPatternLayout">
        <rulesConfig>
          /home/maksym/dev/private-drill/distribution/target/apache-drill-1.20.2.0-eep-SNAPSHOT/apache-drill-1.20.2.0-eep-SNAPSHOT/conf/masking-rules.json
        </rulesConfig>
        <pattern>%d{HH:mm:ss.SSS} [%thread] %-5level %logger{36} - %msg%n</pattern>
      </layout>
    </encoder>
  </appender>

  <root>
    <level value="error"/>
    <appender-ref ref="STDOUT"/>
  </root>
</configuration>
```

They are both have a new option `rulesConfig` where user should provide a path to JSON config file with masking rules.
If
to miss `rulesConfig` option then `MaskingPatternEncoder` and `MaskingPatternLayout` will work just like
`PatternLayoutEncoder` and `PatternLayout`.

Config file contains an array of rules in JSON format:

```json
[
  {
    "search": "([\\w\\d]+\\.)+(com)",
    "replace": "secret.domain.com",
    "description": "Mask domain names",
    "includeUser": "testUser",
    "excludeUser": "excludedUser"
  },
  {
    "search": "MagicCompany",
    "replace": "TopSecretCompany",
    "description": "Mask company name"
  }
]
```   

Where:

1. `search` *optional* - regex (*make sure that proper escaping is used*) pattern of string which is need to
   change/mask.
   If it is empty, `null`, or omitted then the rule will be not active (not used).
2. `replace` *optional* - defines a simple string to be changed to. Can have empty value(`"replace": ""`). Default
   value: `""`
3. `descripton` *optional* - is intended for self-documentation purposes
4. `includeUser` *optional* - regex (*make sure that proper escaping is used*) pattern of user whose messages should be
   changed/masked. By default messages of all users are accepted.
5. `excludeUser` *optional* - regex (*make sure that proper escaping is used*) pattern of user whose messages should NOT
   be changed/masked. `excludeUser` has higher priority than `includeUser`.

Config file can be empty:

```json

```

or with empty array:

```json
[]
```

