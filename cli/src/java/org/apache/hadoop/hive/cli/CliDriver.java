/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.cli;

import com.google.common.base.Splitter;
import jline.console.ConsoleReader;
import jline.console.completer.ArgumentCompleter;
import jline.console.completer.ArgumentCompleter.AbstractArgumentDelimiter;
import jline.console.completer.ArgumentCompleter.ArgumentDelimiter;
import jline.console.completer.Completer;
import jline.console.completer.StringsCompleter;
import jline.console.history.FileHistory;
import jline.console.history.History;
import jline.console.history.PersistentHistory;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.HiveInterruptUtils;
import org.apache.hadoop.hive.common.LogUtils;
import org.apache.hadoop.hive.common.LogUtils.LogInitializationException;
import org.apache.hadoop.hive.common.cli.EscapeCRLFHelper;
import org.apache.hadoop.hive.common.cli.ShellCmdExecutor;
import org.apache.hadoop.hive.common.io.CachingPrintStream;
import org.apache.hadoop.hive.common.io.FetchConverter;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.conf.HiveVariableSource;
import org.apache.hadoop.hive.conf.Validator;
import org.apache.hadoop.hive.conf.VariableSubstitution;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.IDriver;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.exec.mr.HadoopJobExecHelper;
import org.apache.hadoop.hive.ql.exec.tez.TezJobExecHelper;
import org.apache.hadoop.hive.ql.metadata.HiveMaterializedViewsRegistry;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.processors.CommandProcessor;
import org.apache.hadoop.hive.ql.processors.CommandProcessorFactory;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.session.SessionState.LogHelper;
import org.apache.hadoop.io.IOUtils;
import org.apache.hive.common.util.HiveStringUtils;
import org.apache.hive.common.util.ShutdownHookManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.Signal;
import sun.misc.SignalHandler;

import java.io.*;
import java.sql.SQLException;
import java.util.*;

import static org.apache.hadoop.util.StringUtils.stringifyException;


/**
 * CliDriver.
 */
public class CliDriver {

    public static String prompt = null;
    public static String prompt2 = null; // when ';' is not yet seen
    public static final int LINES_TO_FETCH = 40; // number of lines to fetch in batch from remote hive server
    public static final int DELIMITED_CANDIDATE_THRESHOLD = 10;

    public static final String HIVERCFILE = ".hiverc";

    private final LogHelper console;
    protected ConsoleReader reader;
    private Configuration conf;

    public CliDriver() {
        SessionState ss = SessionState.get();
        conf = (ss != null) ? ss.getConf() : new Configuration();
        Logger LOG = LoggerFactory.getLogger("CliDriver");
        if (LOG.isDebugEnabled()) {
            LOG.debug("CliDriver inited with classpath {}", System.getProperty("java.class.path"));
        }
        console = new LogHelper(LOG);
    }

    /**
     * 处理一条独立的HQL和执行工作
     *
     * @param cmd
     * @return
     */
    public int processCmd(String cmd) {
        CliSessionState ss = (CliSessionState) SessionState.get();
        ss.setLastCommand(cmd);

        ss.updateThreadName();

        // 刷新打印流，使其不包括上一个命令的输出
        ss.err.flush();
        String cmd_trimmed = HiveStringUtils.removeComments(cmd).trim();
        String[] tokens = tokenizeCmd(cmd_trimmed);
        int ret = 0;
        // TODO: 处理SQL的命令  对应4个if语句
        /**
         * 1、select 。。。
         * 2、exit  quit
         * 3、source 。。。
         * 4、！shell
         *
         *
         */
        //如果是推出
        if (cmd_trimmed.toLowerCase().equals("quit") || cmd_trimmed.toLowerCase().equals("exit")) {

            // if we have come this far - either the previous commands
            // are all successful or this is command line. in either case
            // this counts as a successful run
            ss.close();
            System.exit(0);

        } else
            // 如果是source
            if (tokens[0].equalsIgnoreCase("source")) {
                String cmd_1 = getFirstCmd(cmd_trimmed, tokens[0].length());
                cmd_1 = new VariableSubstitution(new HiveVariableSource() {
                    @Override
                    public Map<String, String> getHiveVariable() {
                        return SessionState.get().getHiveVariables();
                    }
                }).substitute(ss.getConf(), cmd_1);

                File sourceFile = new File(cmd_1);
                if (!sourceFile.isFile()) {
                    console.printError("File: " + cmd_1 + " is not a file.");
                    ret = 1;
                } else {
                    try {
                        // 处理文件
                        ret = processFile(cmd_1);
                    } catch (IOException e) {
                        console.printError("Failed processing file " + cmd_1 + " " + e.getLocalizedMessage(),
                                stringifyException(e));
                        ret = 1;
                    }
                }
            }
            //如果是！shell
            // hive是支持在hive界面执行HDFS 和 一些shell操作命令的
            else if (cmd_trimmed.startsWith("!")) {
                // for shell commands, use unstripped command
                String shell_cmd = cmd.trim().substring(1);
                shell_cmd = new VariableSubstitution(new HiveVariableSource() {
                    @Override
                    public Map<String, String> getHiveVariable() {
                        return SessionState.get().getHiveVariables();
                    }
                }).substitute(ss.getConf(), shell_cmd);
                //处理和执行一个 shell 操作
                // shell_cmd = "/bin/bash -c \'" + shell_cmd + "\'";
                try {
                    ShellCmdExecutor executor = new ShellCmdExecutor(shell_cmd, ss.out, ss.err);
                    ret = executor.execute();
                    if (ret != 0) {
                        console.printError("Command failed with exit code = " + ret);
                    }
                } catch (Exception e) {
                    console.printError("Exception raised from Shell command " + e.getLocalizedMessage(),
                            stringifyException(e));
                    ret = 1;
                }
            }
            //处理正常的HQL语句
            else { // local mode
                try {
                    // 获取处理器
                    try (CommandProcessor proc = CommandProcessorFactory.get(tokens, (HiveConf) conf)) {
                        if (proc instanceof IDriver) {
                            // 让驱动程序使用sql解析器剥离
                            // TODO:  处理HQL
                            /**
                             * cmd：HQL
                             * proc：处理器对象
                             * ss：会话
                             */
                            ret = processLocalCmd(cmd, proc, ss);
                        } else {
                            ret = processLocalCmd(cmd_trimmed, proc, ss);
                        }
                    }
                } catch (SQLException e) {
                    console.printError("Failed processing command " + tokens[0] + " " + e.getLocalizedMessage(),
                            org.apache.hadoop.util.StringUtils.stringifyException(e));
                    ret = 1;
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }

        ss.resetThreadName();
        return ret;
    }

    /**
     * For testing purposes to inject Configuration dependency
     *
     * @param conf to replace default
     */
    void setConf(Configuration conf) {
        this.conf = conf;
    }

    /**
     * Extract and clean up the first command in the input.
     */
    private String getFirstCmd(String cmd, int length) {
        return cmd.substring(length).trim();
    }

    private String[] tokenizeCmd(String cmd) {
        return cmd.split("\\s+");
    }

    /**
     * 核心的执行逻辑分为4个步骤
     * 1、执行sql 获得结果
     * 2、打印表头
     * 3、输出  结果数据
     * 4、输出任务相关信息
     */
    /**
     * 完整的执行一个HQL命令
     * 有重试机制
     * @param cmd
     * @param proc
     * @param ss
     * @return
     */
    int processLocalCmd(String cmd, CommandProcessor proc, CliSessionState ss) {
        boolean escapeCRLF = HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_CLI_PRINT_ESCAPE_CRLF);
        int ret = 0;

        if (proc != null) {
            if (proc instanceof IDriver) {
                IDriver qp = (IDriver) proc;
                PrintStream out = ss.out;
                long start = System.currentTimeMillis();
                if (ss.getIsVerbose()) {
                    out.println(cmd);
                }
                /**
                 * todo 1、执行HQL拿到结果
                 * 如果ret有值了，这个方法返回，就证明对应的SQL执行结果已经有了
                 */
                ret = qp.run(cmd).getResponseCode();
                if (ret != 0) {
                    qp.close();
                    return ret;
                }

                // 查询已运行时间
                long end = System.currentTimeMillis();
                double timeTaken = (end - start) / 1000.0;

                ArrayList<String> res = new ArrayList<String>();
                /**
                 * todo 2、打印表头
                 * 前提是设置了set hive.cli.print.header=true;  则会输出字段名信息
                 */
                printHeader(qp, out);

                // 打印结果
                int counter = 0;
                try {
                    if (out instanceof FetchConverter) {
                        ((FetchConverter) out).fetchStarted();
                    }
                    /**
                     * 3、输入打印HQL的执行结果
                     */
                    while (qp.getResults(res)) {
                        for (String r : res) {
                            if (escapeCRLF) {
                                r = EscapeCRLFHelper.escapeCRLF(r);
                            }
                            //这里是打印的结果
                            out.println(r);
                        }
                        counter += res.size();
                        res.clear();
                        if (out.checkError()) {
                            break;
                        }
                    }
                } catch (IOException e) {
                    console.printError("Failed with exception " + e.getClass().getName() + ":" + e.getMessage(),
                            "\n" + org.apache.hadoop.util.StringUtils.stringifyException(e));
                    ret = 1;
                }

                qp.close();

                if (out instanceof FetchConverter) {
                    ((FetchConverter) out).fetchFinished();
                }
                /**
                 * todo 4、执行完成后这里会打印一个耗时的时间以及有多少条数据，
                 */
                console.printInfo(
                        "Time taken: " + timeTaken + " seconds" + (counter == 0 ? "" : ", Fetched: " + counter + " row(s)"));
            } else {
                String firstToken = tokenizeCmd(cmd.trim())[0];
                String cmd_1 = getFirstCmd(cmd.trim(), firstToken.length());

                if (ss.getIsVerbose()) {
                    ss.out.println(firstToken + " " + cmd_1);
                }
                CommandProcessorResponse res = proc.run(cmd_1);
                if (res.getResponseCode() != 0) {
                    ss.out
                            .println("Query returned non-zero code: " + res.getResponseCode() + ", cause: " + res.getErrorMessage());
                }
                if (res.getConsoleMessages() != null) {
                    for (String consoleMsg : res.getConsoleMessages()) {
                        console.printInfo(consoleMsg);
                    }
                }
                ret = res.getResponseCode();
            }
        }

        return ret;
    }

    /**
     * If enabled and applicable to this command, print the field headers
     * for the output.
     *
     * @param qp  Driver that executed the command
     * @param out PrintStream which to send output to
     */
    private void printHeader(IDriver qp, PrintStream out) {
        List<FieldSchema> fieldSchemas = qp.getSchema().getFieldSchemas();
        if (HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_CLI_PRINT_HEADER)
                && fieldSchemas != null) {
            // Print the column names
            boolean first_col = true;
            for (FieldSchema fs : fieldSchemas) {
                if (!first_col) {
                    out.print('\t');
                }
                out.print(fs.getName());
                first_col = false;
            }
            out.println();
        }
    }

    public int processLine(String line) {
        return processLine(line, false);
    }

    /**
     * Processes a line of semicolon separated commands
     *
     * @param line              要处理的命令 也就是 由；拼接的sql或者命令
     * @param allowInterrupting When true the function will handle SIG_INT (Ctrl+C) by interrupting the processing and
     *                          returning -1
     * @return 0 if ok
     */
    public int processLine(String line, boolean allowInterrupting) {
        SignalHandler oldSignal = null;
        Signal interruptSignal = null;
        // TODO: 判断是否是中断的操作  CTRL+c
        if (allowInterrupting) {
            // 记住我们开始行处理时运行的所有线程。处理此行时连接自定义Ctrl+C处理程序
            interruptSignal = new Signal("INT");
            oldSignal = Signal.handle(interruptSignal, new SignalHandler() {
                private boolean interruptRequested;

                @Override
                public void handle(Signal signal) {
                    boolean initialRequest = !interruptRequested;
                    interruptRequested = true;

                    // 在第二次ctrl+c时终止VM
                    if (!initialRequest) {
                        console.printInfo("Exiting the JVM");
                        System.exit(127);
                    }

                    // 中断CLI线程以停止当前语句并返回提示符
                    console.printInfo("Interrupting... Be patient, this might take some time.");
                    console.printInfo("Press Ctrl+C again to kill JVM");

                    // 首先，干掉任何在运行的job
                    HadoopJobExecHelper.killRunningJobs();
                    TezJobExecHelper.killRunningJobs();
                    HiveInterruptUtils.interrupt();
                }
            });
        }

        try {
            int lastRet = 0, ret = 0;
            // TODO: 把由分号拼接的HQL字符串 切成多个HQL
            // 我们不能将“split”函数直接用作“；”可引用

            List<String> commands = splitSemiColon(line);
            // 下面每次commit都是提交一条单独的HQ了
            String command = "";
            for (String oneCmd : commands) {

                if (StringUtils.endsWith(oneCmd, "\\")) {
                    command += StringUtils.chop(oneCmd) + ";";
                    continue;
                } else {
                    command += oneCmd;
                }
                if (StringUtils.isBlank(command)) {
                    continue;
                }
                // TODO: 处理一条独立的HQL
                ret = processCmd(command);
                command = "";
                lastRet = ret;
                boolean ignoreErrors = HiveConf.getBoolVar(conf, HiveConf.ConfVars.CLIIGNOREERRORS);
                if (ret != 0 && !ignoreErrors) {
                    return ret;
                }
            }
            return lastRet;
        } finally {
            // Once we are done processing the line, restore the old handler
            if (oldSignal != null && interruptSignal != null) {
                Signal.handle(interruptSignal, oldSignal);
            }
        }
    }

    public static List<String> splitSemiColon(String line) {
        boolean insideSingleQuote = false;
        boolean insideDoubleQuote = false;
        boolean escape = false;
        int beginIndex = 0;
        List<String> ret = new ArrayList<>();
        for (int index = 0; index < line.length(); index++) {
            if (line.charAt(index) == '\'') {
                // take a look to see if it is escaped
                if (!escape) {
                    // flip the boolean variable
                    insideSingleQuote = !insideSingleQuote;
                }
            } else if (line.charAt(index) == '\"') {
                // take a look to see if it is escaped
                if (!escape) {
                    // flip the boolean variable
                    insideDoubleQuote = !insideDoubleQuote;
                }
            } else if (line.charAt(index) == ';') {
                if (insideSingleQuote || insideDoubleQuote) {
                    // do not split
                } else {
                    // split, do not include ; itself
                    ret.add(line.substring(beginIndex, index));
                    beginIndex = index + 1;
                }
            } else {
                // nothing to do
            }
            // set the escape
            if (escape) {
                escape = false;
            } else if (line.charAt(index) == '\\') {
                escape = true;
            }
        }
        ret.add(line.substring(beginIndex));
        return ret;
    }

    public int processReader(BufferedReader r) throws IOException {
        String line;
        StringBuilder qsb = new StringBuilder();

        while ((line = r.readLine()) != null) {
            // Skipping through comments
            if (!line.startsWith("--")) {
                qsb.append(line + "\n");
            }
        }

        return (processLine(qsb.toString()));
    }

    public int processFile(String fileName) throws IOException {
        Path path = new Path(fileName);
        FileSystem fs;
        if (!path.toUri().isAbsolute()) {
            fs = FileSystem.getLocal(conf);
            path = fs.makeQualified(path);
        } else {
            fs = FileSystem.get(path.toUri(), conf);
        }
        BufferedReader bufferReader = null;
        int rc = 0;
        try {
            bufferReader = new BufferedReader(new InputStreamReader(fs.open(path)));
            rc = processReader(bufferReader);
        } finally {
            IOUtils.closeStream(bufferReader);
        }
        return rc;
    }

    public void processInitFiles(CliSessionState ss) throws IOException {
        boolean saveSilent = ss.getIsSilent();
        ss.setIsSilent(true);
        for (String initFile : ss.initFiles) {
            int rc = processFile(initFile);
            if (rc != 0) {
                System.exit(rc);
            }
        }
        if (ss.initFiles.size() == 0) {
            if (System.getenv("HIVE_HOME") != null) {
                String hivercDefault = System.getenv("HIVE_HOME") + File.separator +
                        "bin" + File.separator + HIVERCFILE;
                if (new File(hivercDefault).exists()) {
                    int rc = processFile(hivercDefault);
                    if (rc != 0) {
                        System.exit(rc);
                    }
                    console.printError("Putting the global hiverc in " +
                            "$HIVE_HOME/bin/.hiverc is deprecated. Please " +
                            "use $HIVE_CONF_DIR/.hiverc instead.");
                }
            }
            if (System.getenv("HIVE_CONF_DIR") != null) {
                String hivercDefault = System.getenv("HIVE_CONF_DIR") + File.separator
                        + HIVERCFILE;
                if (new File(hivercDefault).exists()) {
                    int rc = processFile(hivercDefault);
                    if (rc != 0) {
                        System.exit(rc);
                    }
                }
            }
            if (System.getProperty("user.home") != null) {
                String hivercUser = System.getProperty("user.home") + File.separator +
                        HIVERCFILE;
                if (new File(hivercUser).exists()) {
                    int rc = processFile(hivercUser);
                    if (rc != 0) {
                        System.exit(rc);
                    }
                }
            }
        }
        ss.setIsSilent(saveSilent);
    }

    public void processSelectDatabase(CliSessionState ss) throws IOException {
        String database = ss.database;
        if (database != null) {
            int rc = processLine("use " + database + ";");
            if (rc != 0) {
                System.exit(rc);
            }
        }
    }

    public static Completer[] getCommandCompleter() {
        // StringsCompleter matches against a pre-defined wordlist
        // We start with an empty wordlist and build it up
        List<String> candidateStrings = new ArrayList<String>();

        // We add Hive function names
        // For functions that aren't infix operators, we add an open
        // parenthesis at the end.
        for (String s : FunctionRegistry.getFunctionNames()) {
            if (s.matches("[a-z_]+")) {
                candidateStrings.add(s + "(");
            } else {
                candidateStrings.add(s);
            }
        }

        // We add Hive keywords, including lower-cased versions
        for (String s : HiveParser.getKeywords()) {
            candidateStrings.add(s);
            candidateStrings.add(s.toLowerCase());
        }

        StringsCompleter strCompleter = new StringsCompleter(candidateStrings);

        // Because we use parentheses in addition to whitespace
        // as a keyword delimiter, we need to define a new ArgumentDelimiter
        // that recognizes parenthesis as a delimiter.
        ArgumentDelimiter delim = new AbstractArgumentDelimiter() {
            @Override
            public boolean isDelimiterChar(CharSequence buffer, int pos) {
                char c = buffer.charAt(pos);
                return (Character.isWhitespace(c) || c == '(' || c == ')' ||
                        c == '[' || c == ']');
            }
        };

        // The ArgumentCompletor allows us to match multiple tokens
        // in the same line.
        final ArgumentCompleter argCompleter = new ArgumentCompleter(delim, strCompleter);
        // By default ArgumentCompletor is in "strict" mode meaning
        // a token is only auto-completed if all prior tokens
        // match. We don't want that since there are valid tokens
        // that are not in our wordlist (eg. table and column names)
        argCompleter.setStrict(false);

        // ArgumentCompletor always adds a space after a matched token.
        // This is undesirable for function names because a space after
        // the opening parenthesis is unnecessary (and uncommon) in Hive.
        // We stack a custom Completor on top of our ArgumentCompletor
        // to reverse this.
        Completer customCompletor = new Completer() {
            @Override
            public int complete(String buffer, int offset, List completions) {
                List<String> comp = completions;
                int ret = argCompleter.complete(buffer, offset, completions);
                // ConsoleReader will do the substitution if and only if there
                // is exactly one valid completion, so we ignore other cases.
                if (completions.size() == 1) {
                    if (comp.get(0).endsWith("( ")) {
                        comp.set(0, comp.get(0).trim());
                    }
                }
                return ret;
            }
        };

        List<String> vars = new ArrayList<String>();
        for (HiveConf.ConfVars conf : HiveConf.ConfVars.values()) {
            vars.add(conf.varname);
        }

        StringsCompleter confCompleter = new StringsCompleter(vars) {
            @Override
            public int complete(final String buffer, final int cursor, final List<CharSequence> clist) {
                int result = super.complete(buffer, cursor, clist);
                if (clist.isEmpty() && cursor > 1 && buffer.charAt(cursor - 1) == '=') {
                    HiveConf.ConfVars var = HiveConf.getConfVars(buffer.substring(0, cursor - 1));
                    if (var == null) {
                        return result;
                    }
                    if (var.getValidator() instanceof Validator.StringSet) {
                        Validator.StringSet validator = (Validator.StringSet) var.getValidator();
                        clist.addAll(validator.getExpected());
                    } else if (var.getValidator() != null) {
                        clist.addAll(Arrays.asList(var.getValidator().toDescription(), ""));
                    } else {
                        clist.addAll(Arrays.asList("Expects " + var.typeString() + " type value", ""));
                    }
                    return cursor;
                }
                if (clist.size() > DELIMITED_CANDIDATE_THRESHOLD) {
                    Set<CharSequence> delimited = new LinkedHashSet<CharSequence>();
                    for (CharSequence candidate : clist) {
                        Iterator<String> it = Splitter.on(".").split(
                                candidate.subSequence(cursor, candidate.length())).iterator();
                        if (it.hasNext()) {
                            String next = it.next();
                            if (next.isEmpty()) {
                                next = ".";
                            }
                            candidate = buffer != null ? buffer.substring(0, cursor) + next : next;
                        }
                        delimited.add(candidate);
                    }
                    clist.clear();
                    clist.addAll(delimited);
                }
                return result;
            }
        };

        StringsCompleter setCompleter = new StringsCompleter("set") {
            @Override
            public int complete(String buffer, int cursor, List<CharSequence> clist) {
                return buffer != null && buffer.equals("set") ? super.complete(buffer, cursor, clist) : -1;
            }
        };

        ArgumentCompleter propCompleter = new ArgumentCompleter(setCompleter, confCompleter) {
            @Override
            public int complete(String buffer, int offset, List<CharSequence> completions) {
                int ret = super.complete(buffer, offset, completions);
                if (completions.size() == 1) {
                    completions.set(0, ((String) completions.get(0)).trim());
                }
                return ret;
            }
        };
        return new Completer[]{propCompleter, customCompletor};
    }

    /**
     * // todo 程序的入口，Driver的main方法
     *
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        // todo Hive的SQL执行入口
        //实例一个客户端驱动
        //返回值是代码,如0正常退出,5就是在控制台Ctrl+c
        int ret = new CliDriver().run(args);
        System.exit(ret);
    }

    /**
     * todo run()方法
     * SQL的样式:
     *  1、select * from xxx
     *  2、hive -e "select * from xxx"
     *  3、hive -f xxx.sql
     *  4、source  xxx
     *
     * @param args
     * @return
     * @throws Exception
     */
    public int run(String[] args) throws Exception {
        /**
         * 实例解析参数的类
         * 主要解析hive.conf等配置文件
         */
        OptionsProcessor oproc = new OptionsProcessor();
        //解析参数的方法
        if (!oproc.process_stage1(args)) {
            return 1;
        }

        // 注释：log4j的初始化,在其他类初始化之前做
        // NOTE: It is critical to do this here so that log4j is reinitialized
        // before any of the other core hive classes are loaded
        boolean logInitFailed = false;
        String logInitDetailMessage;
        try {
            logInitDetailMessage = LogUtils.initHiveLog4j();
        } catch (LogInitializationException e) {
            logInitFailed = true;
            logInitDetailMessage = e.getMessage();
        }
        // TODO: 初始化会话 SessionState
        // new hiveConf()的时候  会进行各种参数的初始化和解析操作
        // hiveConf是真个hive中的配置管理对象
        CliSessionState ss = new CliSessionState(new HiveConf(SessionState.class));
        //初始化各种系统的输入输出，负责接受SQL和打印执行结果
        // 接受用户的sql之后，必将会有一些提示信息需要打印，这些打印配置都在下面这几行代码里
        ss.in = System.in;
        try {
            ss.out = new PrintStream(System.out, true, "UTF-8");
            ss.info = new PrintStream(System.err, true, "UTF-8");
            ss.err = new CachingPrintStream(System.err, true, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            return 3;
        }
        // 解析 -i -f -e -S -H -hivevar 等
        if (!oproc.process_stage2(ss)) {
            return 2;
        }
        //判断是否未静默模式
        //  静默模式：指定后不显示执行进度信息，最后只显示结果
        if (!ss.getIsSilent()) {
            if (logInitFailed) {
                System.err.println(logInitDetailMessage);
            } else {
                SessionState.getConsole().printInfo(logInitDetailMessage);
            }
        }

        // 设置通过命令行指定的所有属性
        HiveConf conf = ss.getConf();
        for (Map.Entry<Object, Object> item : ss.cmdProperties.entrySet()) {
            conf.set((String) item.getKey(), (String) item.getValue());
            ss.getOverriddenConfigurations().put((String) item.getKey(), (String) item.getValue());
        }

        // 读取提示配置并替换变量。
        prompt = conf.getVar(HiveConf.ConfVars.CLIPROMPT);
        prompt = new VariableSubstitution(new HiveVariableSource() {
            @Override
            public Map<String, String> getHiveVariable() {
                return SessionState.get().getHiveVariables();
            }
        }).substitute(conf, prompt);
        prompt2 = spacesForString(prompt);
        // TODO: 启动会话
        if (HiveConf.getBoolVar(conf, ConfVars.HIVE_CLI_TEZ_SESSION_ASYNC)) {
            // 以一种即发即弃的方式开始会话。当需要会话的异步初始化部分时，相应的getter和其他方法将根据需要等待。
            SessionState.beginStart(ss, console);
        } else {
            SessionState.start(ss);
        }

        ss.updateThreadName();

        // 创建视图注册表
        HiveMaterializedViewsRegistry.get().init();

        // 执行cli驱动程序工作
        try {
            // TODO: 开始执行
            return executeDriver(ss, conf, oproc);
        } finally {
            ss.resetThreadName();
            ss.close();
        }
    }

    /**
     * Execute the cli work
     *
     * @param ss    CliSessionState of the CLI driver
     * @param conf  HiveConf for the driver session
     * @param oproc Operation processor of the CLI invocation
     * @return status of the CLI command execution
     * @throws Exception
     */
    private int executeDriver(CliSessionState ss, HiveConf conf, OptionsProcessor oproc)
            throws Exception {
        // TODO: 构建CliDriver 并设置各种参数和变量参数
        CliDriver cli = new CliDriver();
        cli.setHiveVariables(oproc.getHiveVariables());

        // TODO: 如果有 use database语句 则执行
        //如果是  select * from db.table 则另外处理
        //如果指定，请使用指定的数据库
        cli.processSelectDatabase(ss);

        // TODO: 如果hive -i 携带了配置参数文件  则执行这个文件
        // 一般文件中都是  key=value的形式
        // 执行-i初始化文件（始终处于静默模式）
        cli.processInitFiles(ss);

        // TODO: 如果是单行命令  比如hive -e “xxxx” 则执行
        // ss.execString就是 -e 后面带的语句
        if (ss.execString != null) {
            int cmdProcessStatus = cli.processLine(ss.execString);
            return cmdProcessStatus;
        }
        // TODO: 如果是sql文件  比如hive -f xxxx.sql 则执行这里
        try {
            if (ss.fileName != null) {
                return cli.processFile(ss.fileName);
            }
        } catch (FileNotFoundException e) {
            System.err.println("Could not open input file for reading. (" + e.getMessage() + ")");
            return 3;
        }
        // TODO: 引擎如果是MR的话，这里会打印一个警告
        if ("mr".equals(HiveConf.getVar(conf, ConfVars.HIVE_EXECUTION_ENGINE))) {
            console.printInfo(HiveConf.generateMrDeprecationWarning());
        }

        // TODO: 初始化，读取用户在hive的控制台输如的sql或者命令。
        setupConsoleReader();

        String line;
        int ret = 0;
        String prefix = "";
        String curDB = getFormattedDb(conf, ss);
        String curPrompt = prompt + curDB;
        String dbSpaces = spacesForString(curDB);
        // TODO: 不停的读取用户输入的sql
        /**
         * line就是你输入的sql语句
         * 所谓的交互式其实就是下面的这个while死循环，只要jvm不推出，就一直在这运行
         */
        while ((line = reader.readLine(curPrompt + "> ")) != null) {
            //处理特殊符号  如 \n \\ ; 这些
            if (!prefix.equals("")) {
                prefix += '\n';
            }
            // 处理注释
            if (line.trim().startsWith("--")) {
                continue;
            }
            // 如果是以；结尾， 则结束
            if (line.trim().endsWith(";") && !line.trim().endsWith("\\;")) {
                line = prefix + line;
                // todo 处理HQL： 这个可能是一条失SQL也可能是多条
                ret = cli.processLine(line, true);
                prefix = "";
                curDB = getFormattedDb(conf, ss);
                curPrompt = prompt + curDB;
                dbSpaces = dbSpaces.length() == curDB.length() ? dbSpaces : spacesForString(curDB);
            } else {
                prefix = prefix + line;
                curPrompt = prompt2 + dbSpaces;
                continue;
            }
        }

        return ret;
    }

    private void setupCmdHistory() {
        final String HISTORYFILE = ".hivehistory";
        String historyDirectory = System.getProperty("user.home");
        PersistentHistory history = null;
        try {
            if ((new File(historyDirectory)).exists()) {
                String historyFile = historyDirectory + File.separator + HISTORYFILE;
                history = new FileHistory(new File(historyFile));
                reader.setHistory(history);
            } else {
                System.err.println("WARNING: Directory for Hive history file: " + historyDirectory +
                        " does not exist.   History will not be available during this session.");
            }
        } catch (Exception e) {
            System.err.println("WARNING: Encountered an error while trying to initialize Hive's " +
                    "history file.  History will not be available during this session.");
            System.err.println(e.getMessage());
        }

        // add shutdown hook to flush the history to history file
        ShutdownHookManager.addShutdownHook(new Runnable() {
            @Override
            public void run() {
                History h = reader.getHistory();
                if (h instanceof FileHistory) {
                    try {
                        ((FileHistory) h).flush();
                    } catch (IOException e) {
                        System.err.println("WARNING: Failed to write command history file: " + e.getMessage());
                    }
                }
            }
        });
    }

    protected void setupConsoleReader() throws IOException {
        reader = new ConsoleReader();
        reader.setExpandEvents(false);
        reader.setBellEnabled(false);
        for (Completer completer : getCommandCompleter()) {
            reader.addCompleter(completer);
        }
        setupCmdHistory();
    }

    /**
     * Retrieve the current database name string to display, based on the
     * configuration value.
     *
     * @param conf storing whether or not to show current db
     * @param ss   CliSessionState to query for db name
     * @return String to show user for current db value
     */
    private static String getFormattedDb(HiveConf conf, CliSessionState ss) {
        if (!HiveConf.getBoolVar(conf, HiveConf.ConfVars.CLIPRINTCURRENTDB)) {
            return "";
        }
        //BUG: This will not work in remote mode - HIVE-5153
        String currDb = SessionState.get().getCurrentDatabase();

        if (currDb == null) {
            return "";
        }

        return " (" + currDb + ")";
    }

    /**
     * Generate a string of whitespace the same length as the parameter
     *
     * @param s String for which to generate equivalent whitespace
     * @return Whitespace
     */
    private static String spacesForString(String s) {
        if (s == null || s.length() == 0) {
            return "";
        }
        return String.format("%1$-" + s.length() + "s", "");
    }

    public void setHiveVariables(Map<String, String> hiveVariables) {
        SessionState.get().setHiveVariables(hiveVariables);
    }

}
