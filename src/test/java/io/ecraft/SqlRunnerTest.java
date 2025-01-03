package io.ecraft;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.description.Description;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.json.JSONObject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.util.HashMap;

import io.ecraft.SqlRunner;

import static org.junit.jupiter.api.Assertions.assertEquals;

class SqlRunnerTest {
  @Test
  public void testEnvironment() {
    Map<String, String> ctx = SqlRunner.loadEnvironment();

    assertEquals(ctx.get("PWD"), System.getProperty("user.dir"));
  }

  @Test
  public void testTemplating() throws Exception {
    Map<String, String> ctx = new HashMap<String, String>();

    ctx.put("FLINK_TABLE", "T");

    String sql = SqlRunner.formatSqlFile("SELECT * FROM $FLINK_TABLE;", ctx);

    assertEquals(sql, "SELECT * FROM T;\n");
  }

  @Test
  public void testTableConfig() throws Exception {
    EnvironmentSettings settings = EnvironmentSettings
        .newInstance()
        .inStreamingMode()
        .build();
    TableEnvironment tableEnv = TableEnvironment.create(settings);

    String filePath = "src/test/java/io/ecraft/fixtures/deployableconfig1.json";
    JSONObject jsonConfig = readJsonFile(filePath);

    SqlRunner.configureTableEnvironment("dev", jsonConfig, tableEnv);

    // Correctly define the ConfigOption
    ConfigOption<Boolean> miniBatchEnabled = ConfigOptions.key("table.exec.mini-batch.enabled")
      .booleanType()
      .defaultValue(false)
      .withDescription("Enable mini-batch execution.");

    assertEquals(tableEnv.getConfig().getConfiguration().get(miniBatchEnabled), true);

    // define config option for table.exec.source.idle-timeout duration
    ConfigOption<String> sourceIdleTimeout = ConfigOptions.key("table.exec.source.idle-timeout")
      .stringType()
      .defaultValue("0")
      .withDescription("The time that a source will wait for new data before shutting down.");
    
    assertEquals(tableEnv.getConfig().getConfiguration().get(sourceIdleTimeout), "5 min");
  }

  @Test
  public void testEnvironmentConfig() throws Exception {
    EnvironmentSettings.Builder builder = EnvironmentSettings.newInstance();

    String filePath = "src/test/java/io/ecraft/fixtures/deployableconfig1.json";
    JSONObject jsonConfig = readJsonFile(filePath);

    EnvironmentSettings settings = SqlRunner.configureEnvironmentSettings("dev", jsonConfig, builder).build();

    assertEquals(settings.isStreamingMode(), true);
  }

  public static JSONObject readJsonFile(String filePath) throws IOException {
    String content = new String(Files.readAllBytes(Paths.get(filePath)));
    return new JSONObject(content);
  }

}
