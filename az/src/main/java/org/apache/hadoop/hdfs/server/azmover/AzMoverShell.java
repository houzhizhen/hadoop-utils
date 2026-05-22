package org.apache.hadoop.hdfs.server.azmover;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.server.azmover.AzMoverParams.Builder;
import org.apache.hadoop.hdfs.server.azmover.FileStatusVisitor.FileStatusFilter;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AzMoverShell implements Tool {

  static final Logger LOG = LoggerFactory.getLogger(AzMoverShell.class);

  private static final Option HELP = new Option("h", "help", false, "Print usage");
  private static final Option PATH = new Option("p", "path", true, "Path to mover");
  private static final Option INPUT = new Option("i", "input", true,
      "Input file or directory of paths, the format of file must be csv and one path per line. Option '--path' will be ignored when input is configured.");
  private static final Option REPLICATION = new Option("r", "replication", true,
      "Target replication");
  private static final Option NAME_SERVICE = new Option("n", "nameService", true, "Name service");
  private static final Option PARALLELISM = new Option("l", "parallelism", true,
      "The parallelism of mover, default 1");
  private static final Option EXCLUDED_PATHS = new Option("e", "excludedPaths", true,
      "Excluded paths, can be a regex pattern, default none");
  private static final Option LISTENING_THREAD = new Option("t", "listeningThread", true,
      "The parallelism of listening directory, default 1");
  private static final Option FILTERS = new Option("f", "filters", true,
      "The filter of file, default none. Supported filters are:\n"
          + "- 'replication=${rep}', moving files with replication=${rep}, like: 'replication=2';\n"
          + "- 'mtimeUpperBound=${time}', moving files with mtime<=${time}, like: 'mtimeUpperBound=2022-01-01T00:00:00';\n"
          + "- 'mtimeLowerBound=${time}', moving files with mtime>=${time}, like: 'mtimeLowerBound=2022-01-01T00:00:00';\n"
          + "- 'lengthUpperBound=${length}', moving files with length<=${length}, like: 'lengthUpperBound=1048576';\n"
          + "- 'lengthLowerBound=${length}', moving files with length>=${length}, like: 'lengthLowerBound=1024';\n"
          + "- 'fileType=${type}', moving files with file type, like: fileType=ec, fileType=normal, fileType=all.\n"
  );
  private static final Option NODE_SELECTOR = new Option("s", "nodeSelector", true,
      "Select spec number nodes as copy target for each az, like '/az1=1,/az2=3,/az3=2', default none. ");
  private static final Option MATCH_AZ_POLICY = new Option("m", "matchAzPolicy", true,
      "Only move blocks with spec azPolicy, like '/az1,/az1,/az1'. This option is invalid for ec block.");

  private static final Option SKIP_CONFIG_CHECK = new Option("c", "skipConfigCheck", false,
      "Whether skip check config, it would be useful for crontab.");

  static {
    REPLICATION.setRequired(true);
    NAME_SERVICE.setRequired(true);
  }


  private static Options buildCliOptions() {
    Options options = new Options();
    options.addOption(HELP);
    options.addOption(PATH);
    options.addOption(INPUT);
    options.addOption(REPLICATION);
    options.addOption(NAME_SERVICE);
    options.addOption(PARALLELISM);
    options.addOption(EXCLUDED_PATHS);
    options.addOption(LISTENING_THREAD);
    options.addOption(FILTERS);
    options.addOption(NODE_SELECTOR);
    options.addOption(MATCH_AZ_POLICY);
    options.addOption(SKIP_CONFIG_CHECK);
    return options;
  }

  private Configuration conf;

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
    String confDir = System.getenv("HADOOP_CONF_DIR");
    if (StringUtils.isEmpty(confDir)) {
      String hadoopHome = System.getenv("HADOOP_HOME");
      if (StringUtils.isEmpty(hadoopHome)) {
        return;
      }
      confDir = hadoopHome + "/etc/hadoop";
    }

    String hdfsSite = confDir + "/hdfs-site.xml";
    LOG.info("Loading hdfs-site.xml: {}", hdfsSite);
    conf.addResource(new Path(hdfsSite));
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public int run(String[] args) {
    final Options options = buildCliOptions();
    CommandLine cmd;
    try {
      cmd = new BasicParser().parse(options, args);
    } catch (ParseException e) {
      printUsage(options);
      e.printStackTrace();
      return -1;
    }
    if (cmd.hasOption(HELP.getLongOpt())) {
      printUsage(options);
      return 0;
    }

    Builder builder = AzMoverParams.builder()
        .withPath(cmd.getOptionValue(PATH.getLongOpt()))
        .withInput(cmd.getOptionValue(INPUT.getLongOpt()))
        .withTargetRep(Integer.parseInt(cmd.getOptionValue(REPLICATION.getLongOpt())))
        .withNameService(cmd.getOptionValue(NAME_SERVICE.getLongOpt()));

    if (cmd.hasOption(PARALLELISM.getLongOpt())) {
      builder.withParallelism(Integer.parseInt(cmd.getOptionValue(PARALLELISM.getLongOpt())));
    }

    if (cmd.hasOption(EXCLUDED_PATHS.getLongOpt())) {
      builder.withExcludedPaths(
          ImmutableSet.copyOf(cmd.getOptionValues(EXCLUDED_PATHS.getLongOpt())));
    }

    if (cmd.hasOption(LISTENING_THREAD.getLongOpt())) {
      builder.withListeningThread(
          Integer.parseInt(cmd.getOptionValue(LISTENING_THREAD.getLongOpt())));
    }

    if (cmd.hasOption(FILTERS.getLongOpt())) {
      builder.withFilters(parseFilters(cmd.getOptionValues(FILTERS.getLongOpt())));
    }

    if (cmd.hasOption(NODE_SELECTOR.getLongOpt())) {
      builder.withNodeSelector(parseNodeSelector(cmd.getOptionValue(NODE_SELECTOR.getLongOpt())));
    }

    if (cmd.hasOption(MATCH_AZ_POLICY.getLongOpt())) {
      ImmutableList<String> matchAzPolicy = ImmutableList.copyOf(
          cmd.getOptionValue(MATCH_AZ_POLICY.getLongOpt()).split(","));
      builder.withMatchAzPolicy(matchAzPolicy);
    }

    if (cmd.hasOption(SKIP_CONFIG_CHECK.getLongOpt())) {
      builder.skipConfigCheck();
    }

    AzMoverParams params = builder.build();

    if (!params.isSkipConfigCheck()) {
      try (Scanner scanner = new Scanner(System.in)) {
        printEnterMsg(params.toString());
        a:
        while (scanner.hasNext()) {
          String next = scanner.next();
          switch (next) {
            case "y":
            case "Y":
              break a;
            case "n":
            case "N":
              System.exit(0);
            default:
              printEnterMsg(params.toString());
          }
        }
      }
    }

    LOG.info("Run azmover with parameters: \n" + params);
    LOG.info("Azmover started, waiting for finished......");
    return new AzMover(params, conf).run();
  }

  private static void printEnterMsg(String params) {
    String line = String.join("", Collections.nCopies(128, "-"));
    StringBuilder msg = new StringBuilder();
    msg.append(line).append("\n")
        .append("Please check you parameters for azmover:").append("\n")
        .append(params).append("\n")
        .append(line).append("\n")
        .append("Enter [Y/N] to continue or exit:");
    System.out.println(msg);
  }

  private static void printUsage(Options options) {
    HelpFormatter formatter = new HelpFormatter();
    formatter.setWidth(256);
    formatter.printHelp("hadoop azmover [-Dkey=value] [-Dkey=value]", options);
  }

  public static void main(String[] args) {
    try {
      LOG.info("Run azmover shell");
      System.exit(ToolRunner.run(new AzMoverShell(), args));
    } catch (Throwable e) {
      String exitMsg = "Exiting azmover due to an exception";
      LOG.error(exitMsg, e);
      System.err.println(exitMsg + "\n" + org.apache.hadoop.util.StringUtils.stringifyException(e));
      System.exit(-1);
    }
  }

  public static Set<FileStatusFilter> parseFilters(String[] filters) {
    return Arrays.stream(filters)
        .map(s -> s.split("="))
        .peek(split -> Preconditions.checkArgument(split.length == 2, "Error format for filters"))
        .map(pair -> {
          String key = pair[0];
          String value = pair[1];
          switch (key) {
            case "replication":
              short replication = Short.parseShort(value);
              return new FileStatusFilter() {
                @Override
                public boolean isAccepted(LocatedFileStatus fileStatus) {
                  return fileStatus.isDirectory()
                      || fileStatus.getReplication() == replication;
                }

                @Override
                public String description() {
                  return "Moving files with replication = " + value;
                }
              };
            case "mtimeUpperBound":
              long time = Timestamp.valueOf(LocalDateTime.parse(value)).getTime();
              return new FileStatusFilter() {

                @Override
                public boolean isAccepted(LocatedFileStatus fileStatus) {
                  return fileStatus.isDirectory()
                      || fileStatus.getModificationTime() <= time;
                }

                @Override
                public String description() {
                  return "Moving files with modificationTime <= " + value;
                }
              };
            case "mtimeLowerBound":
              time = Timestamp.valueOf(LocalDateTime.parse(value)).getTime();
              return new FileStatusFilter() {

                @Override
                public boolean isAccepted(LocatedFileStatus fileStatus) {
                  return fileStatus.isDirectory()
                      || fileStatus.getModificationTime() >= time;
                }

                @Override
                public String description() {
                  return "Moving files with modificationTime >= " + value;
                }
              };
            case "lengthUpperBound":
              long length = Long.parseLong(value);
              return new FileStatusFilter() {

                @Override
                public boolean isAccepted(LocatedFileStatus fileStatus) {
                  return fileStatus.isDirectory()
                      || fileStatus.getLen() <= length;
                }

                @Override
                public String description() {
                  return "Moving files with length <= " + value;
                }
              };
            case "lengthLowerBound":
              length = Long.parseLong(value);
              return new FileStatusFilter() {

                @Override
                public boolean isAccepted(LocatedFileStatus fileStatus) {
                  return fileStatus.isDirectory()
                      || fileStatus.getLen() >= length;
                }

                @Override
                public String description() {
                  return "Moving files with length >= " + value;
                }
              };
            case "fileType":
              FileType type = FileType.fromString(value);
              return new FileStatusFilter() {
                @Override
                public boolean isAccepted(LocatedFileStatus fileStatus) {
                  if (fileStatus.isDirectory()) {
                    return true;
                  }
                  switch (type) {
                    case ALL:
                      return true;
                    case EC:
                      return fileStatus.isErasureCoded();
                    case NORMAL:
                      return !fileStatus.isErasureCoded();
                    default:
                      throw new IllegalArgumentException("Unsupported file type: " + type);
                  }
                }

                @Override
                public String description() {
                  return "Moving files with file type = " + type;
                }
              };

            default:
              throw new IllegalArgumentException(
                  String.format("Unsupported filter: %s = %s", key, value));
          }
        }).collect(Collectors.toSet());
  }

  public enum FileType {
    EC, NORMAL, ALL;

    public static FileType fromString(String s) {
      return Arrays.stream(values()).filter(type -> type.name().equalsIgnoreCase(s)).findFirst()
          .orElseThrow(() -> new IllegalArgumentException("Unsupported file type: " + s));
    }
  }

  public static Map<String, Integer> parseNodeSelector(String nodeSelector) {
    return Arrays.stream(nodeSelector.split(","))
        .map(s -> s.split("="))
        .peek(split -> Preconditions.checkArgument(split.length == 2,
            "Error format for nodeSelector"))
        .collect(Collectors.toMap(pair -> pair[0], pair -> Integer.parseInt(pair[1])));
  }
}

