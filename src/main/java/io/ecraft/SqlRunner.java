package io.ecraft;

import jdk.jpackage.internal.Log;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogStore;
import org.apache.flink.table.catalog.FileCatalogStore;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.Velocity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


public class SqlRunner {
  private static final Logger LOG = LoggerFactory.getLogger(SqlRunner.class);

  // a statement should end with `;`
  private static final String STATEMENT_DELIMITER = ";";
  private static final String LINE_DELIMITER = "\n";

  private static final String COMMENT_PATTERN = "(--.*)|(((\\/\\*)+?[\\w\\W]+?(\\*\\/)+))";

  public static void main(String[] args) throws Exception {

    if (args.length != 1) {
      throw new Exception("Exactly one argument is expected.");
    }

//    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//    LOG.info("Checkpoint storage: {}",env.getCheckpointConfig().getCheckpointStorage());
//    LOG.info("Checkpointing mode: {}", env.getCheckpointConfig().getCheckpointingMode());
//    LOG.info("Checkpointing interval: {}", env.getCheckpointInterval());
//    StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

    CatalogStore catalogStore = new FileCatalogStore(System.getenv("TABLE_CATALOG_STORE_FILE_PATH"));
    EnvironmentSettings settings = EnvironmentSettings
            .newInstance()
            .inStreamingMode()
            .withCatalogStore(catalogStore)
            .build();
    TableEnvironment tableEnv = TableEnvironment.create(settings);

    tableEnv.useCatalog("hive");
    tableEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);

    LOG.debug("Current catalog: {}", tableEnv.getCurrentCatalog());
    LOG.debug("Current database: {}", tableEnv.getCurrentDatabase());
    LOG.debug("Available tables:");
    
    for (String t: tableEnv.listTables()) {
      LOG.debug(" - {}", t);
    }

    Path sqlPath = new Path(args[0]);
    FileSystem fs = sqlPath.getFileSystem();
    FSDataInputStream inputStream = fs.open(sqlPath);
    InputStreamReader reader = new InputStreamReader(inputStream);
    String script = new BufferedReader(reader).lines().parallel().collect(Collectors.joining("\n"));

    List<String> statements = parseStatements(script, SqlRunner.loadEnvironment());
    for (String statement : statements) {
      LOG.debug("Executing:\n{}", statement);

      tableEnv.executeSql(statement);
    }
  }



  public static Map<String, String> loadEnvironment() {
    return System.getenv();
  }

  public static List<String> parseStatements(String script, Map<String, String> ctx) throws Exception {
    String formatted = formatSqlFile(script, ctx).replaceAll(COMMENT_PATTERN, "");
    List<String> statements = new ArrayList<String>();

    StringBuilder current = null;
    boolean statementSet = false;
    for (String line : formatted.split("\n")) {
      String trimmed = line.trim();

      if (trimmed.chars().allMatch(Character::isWhitespace)) {
        continue;
      }

      if (current == null) {
        current = new StringBuilder();
      }

      if (trimmed.startsWith("EXECUTE STATEMENT SET")) {
        statementSet = true;
      }

      current.append(trimmed);
      current.append("\n");

      if (trimmed.endsWith(STATEMENT_DELIMITER)) {
        if (!statementSet || trimmed.equals("END;")) {
          statements.add(current.toString());
          current = null;
          statementSet = false;
        }
      }
    }
    return statements;
  }

  public static String formatSqlFile(String content, Map<String, String> ctx) throws Exception {
    Velocity.init();

    VelocityContext context = new VelocityContext();

    for (Map.Entry<String, String> entry : ctx.entrySet()) {
      context.put(entry.getKey(), entry.getValue());
      LOG.debug("Adding {}={} to context", entry.getKey(), entry.getValue());
    }

    StringWriter sw = new StringWriter();
    Velocity.evaluate(context, sw, "", content);

    String trimmed = sw.toString().trim();
    StringBuilder formatted = new StringBuilder();
    formatted.append(trimmed);

    if (!trimmed.endsWith(STATEMENT_DELIMITER)) {
      formatted.append(STATEMENT_DELIMITER);
    }

    formatted.append(LINE_DELIMITER);
    return formatted.toString();
  }
}
