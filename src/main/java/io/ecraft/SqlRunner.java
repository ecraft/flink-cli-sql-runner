package io.ecraft;

import org.apache.hadoop.fs.FileUtil;
import java.util.zip.*;
import java.util.*;
import java.util.stream.Collectors;
import java.io.*;
import java.nio.file.*;
import org.apache.commons.io.FilenameUtils;
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

    EnvironmentSettings settings = EnvironmentSettings
            .newInstance()
            .inStreamingMode()
            .build();
    TableEnvironment tableEnv = TableEnvironment.create(settings);

    String name            = "hive";
    String defaultDatabase = "default";
    String hiveConfDir     = "/conf/hive-conf";

    HiveCatalog hive = new HiveCatalog(name, defaultDatabase, hiveConfDir);
    tableEnv.registerCatalog(name, hive);

    // set the HiveCatalog as the current catalog of the session
    tableEnv.useCatalog(name);

    tableEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);

    LOG.debug("Current catalog: {}", tableEnv.getCurrentCatalog());
    LOG.debug("Current database: {}", tableEnv.getCurrentDatabase());
    LOG.debug("Available tables:");
    
    for (String t: tableEnv.listTables()) {
      LOG.debug(" - {}", t);
    }

    // Read the tar file from azure blob store to a local file
    Path remoteArchivePath = new Path(args[0]);
    FileSystem remoteArchiveFs = remoteArchivePath.getFileSystem();
    FSDataInputStream remoteArchiveStream = remoteArchiveFs.open(remoteArchivePath);
    // We name everything after the full name of the archive without extension (including hashes)
    String jobName = FilenameUtils.getBaseName(remoteArchivePath.getName());

    // Make sure we have the directory for the job files
    Files.createDirectories(Paths.get("/tmp/"+ jobName));

    // Download the file into the directory
    String archiveDownloadPath = "/tmp/"+ jobName + "/" + remoteArchivePath.getName();
    FileOutputStream fos = new FileOutputStream(archiveDownloadPath);
    transferTo(remoteArchiveStream, fos);

    // Uncompress the contents of the zip file to a local directory
    ZipFile zipFile = new ZipFile(archiveDownloadPath);
    Enumeration<? extends ZipEntry> zipFileEntries = zipFile.entries();
    while(zipFileEntries.hasMoreElements()) {
      ZipEntry entry = zipFileEntries.nextElement();
      FileOutputStream zipEntryOutputStream = new FileOutputStream("/tmp/" + jobName + "/" + entry.getName());
      InputStream zipInputStream = zipFile.getInputStream(entry);
      transferTo(zipInputStream, zipEntryOutputStream);
    }

    // Read the sql file 
    String sqlName = remoteArchivePath.getName().substring(0, remoteArchivePath.getName().lastIndexOf("-")) + ".sql";
    Path sqlPath = new Path("/tmp/" + jobName + "/" + sqlName);
    FileSystem sqlFs = sqlPath.getFileSystem();
    FSDataInputStream sqlInputStream = sqlFs.open(sqlPath);
    InputStreamReader reader = new InputStreamReader(sqlInputStream);
    String script = new BufferedReader(reader).lines().parallel().collect(Collectors.joining("\n"));

    List<String> statements = parseStatements(script, SqlRunner.loadEnvironment());
    for (String statement : statements) {
      LOG.debug("Executing:\n{}", statement);

      tableEnv.executeSql(statement);
    }
  }

  public static void transferTo(InputStream input, OutputStream output) throws IOException {
    try {
      byte[] buffer = new byte[1024];
      int len = input.read(buffer);
      while (len != -1) {
        output.write(buffer, 0, len);
        len = input.read(buffer);
      }
      input.close();
      output.close();
    } catch (IOException e) {
      LOG.debug("Failed transferTo:\n{}", e.getMessage());
      throw e;
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
