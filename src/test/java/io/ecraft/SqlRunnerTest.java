package io.ecraft;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
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
}
