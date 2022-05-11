package org.apache.tez.common.web;

import com.google.common.base.Joiner;
import com.google.common.io.Files;

import org.apache.hadoop.http.HttpServer2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class ProfileServlet extends HttpServlet {
    private static final long serialVersionUID = 1L; // do we need this?
    private static final Logger LOG = LoggerFactory.getLogger(ProfileServlet.class);
    private static final String ACCESS_CONTROL_ALLOW_METHODS = "Access-Control-Allow-Methods";
    private static final String ALLOWED_METHODS = "GET";
    private static final String ACCESS_CONTROL_ALLOW_ORIGIN = "Access-Control-Allow-Origin";
    private static final String CONTENT_TYPE_TEXT = "text/plain; charset=utf-8";
    private static final String ASYNC_PROFILER_HOME_ENV = "ASYNC_PROFILER_HOME";
    private static final String ASYNC_PROFILER_HOME_SYSTEM_PROPERTY = "async.profiler.home";
    private static final String ASYNC_PROFILER_DEFAULT = "/Users/laszlobodor/Downloads/async-profiler-1.8.7-macos-x64"; //temporarily TODO: find out
    private static final String PROFILER_SCRIPT = "/profiler.sh";
    private static final int DEFAULT_DURATION_SECONDS = 10;
    private static final AtomicInteger ID_GEN = new AtomicInteger(0);
    static final String OUTPUT_DIR = System.getProperty("java.io.tmpdir") + "/prof-output"; //TODO : it is not pointing to the same tmp folder as the /prof-output context's real path
//    static final String OUTPUT_DIR = "/Users/zheenbekakimzhanov/Desktop/work/tez/tez-tests/target/tmp/org.apache.tez.test.TestAM/yarn-385676069/org.apache.tez.test.TestAM-localDir-nm-0_0/usercache/zheenbekakimzhanov/appcache/application_1651662019985_0001/container_1651662019985_0001_01_000001/tmp/jetty-0_0_0_0-50004-hadoop-common-3_3_1-tests_jar-_-any-12605016871911039289/webapp/prof-output";

    private static final byte[] ARRAY = new byte[100000];

    static {
      new Random().nextBytes(ARRAY);
    }

    enum Event {
        CPU("cpu"),
        ALLOC("alloc"),
        LOCK("lock"),
        PAGE_FAULTS("page-faults"),
        CONTEXT_SWITCHES("context-switches"),
        CYCLES("cycles"),
        INSTRUCTIONS("instructions"),
        CACHE_REFERENCES("cache-references"),
        CACHE_MISSES("cache-misses"),
        BRANCHES("branches"),
        BRANCH_MISSES("branch-misses"),
        BUS_CYCLES("bus-cycles"),
        L1_DCACHE_LOAD_MISSES("L1-dcache-load-misses"),
        LLC_LOAD_MISSES("LLC-load-misses"),
        DTLB_LOAD_MISSES("dTLB-load-misses"),
        MEM_BREAKPOINT("mem:breakpoint"),
        TRACE_TRACEPOINT("trace:tracepoint"),
        ;

        private final String internalName;

        Event(final String internalName) {
            this.internalName = internalName;
        }

        public String getInternalName() {
            return internalName;
        }

        public static Event fromInternalName(final String name) {
            for (Event event : values()) {
                if (event.getInternalName().equalsIgnoreCase(name)) {
                    return event;
                }
            }
            return null;
        }
    }

    enum Output {
        SUMMARY,
        TRACES,
        FLAT,
        COLLAPSED,
        SVG,
        TREE,
        JFR
    }

    private final Lock profilerLock = new ReentrantLock();
    private Integer pid = null;
    private String asyncProfilerHome;
    private transient Process process;

    public ProfileServlet() {
        this.asyncProfilerHome = getAsyncProfilerHome();
        String jvm_pid = System.getenv("JVM_PID");
        if (jvm_pid != null && !jvm_pid.trim().isEmpty()) { //TODO : should it be !jvm_pid.trim().isEmpty() instead of jvm_pid.trim().isEmpty() ?
            this.pid = Integer.valueOf(jvm_pid); //ProcessUtils.getPid();
        }
        LOG.info("Servlet process PID: {} asyncProfilerHome: {}", pid, asyncProfilerHome);
    }

    public void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        response.setContentType("text/plain; charset=UTF-8");
        PrintStream out = new PrintStream(response.getOutputStream(), false, "UTF-8"); // TODO : Has to be .closed not to cause error. Why?
        if (!HttpServer2.isInstrumentationAccessAllowed(this.getServletContext(), request, response)) {
            response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
            setResponseHeader(response);
            out.println("Unauthorized: Instrumentation access is not allowed!");
            out.close();
            return;
        }

        // make sure async profiler home is set
        if (asyncProfilerHome == null || asyncProfilerHome.trim().isEmpty()) {
            response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            setResponseHeader(response);
            // TODO : Temporarily set it to default and do not terminate
            out.println("ASYNC_PROFILER_HOME env is not set. Setting to " + ASYNC_PROFILER_DEFAULT);
            asyncProfilerHome = ASYNC_PROFILER_DEFAULT;
//            out.close();
//            return;
        }

        // if pid is explicitly specified, use it else default to current process
        pid = getInteger(request, "pid", pid);
        // if pid is not specified in query param and if current process pid cannot be determined
        if (pid == null) {
            response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            setResponseHeader(response);
            out.println("'pid' query parameter unspecified or unable to determine PID of current process.");
            out.close();
            return;
        }

        final int duration = getInteger(request, "duration", DEFAULT_DURATION_SECONDS);
        final Output output = getOutput(request);
        final Event event = getEvent(request);
        final Long interval = getLong(request, "interval");
        final Integer jstackDepth = getInteger(request, "jstackdepth", null);
        final Long bufsize = getLong(request, "bufsize");
        final boolean thread = request.getParameterMap().containsKey("thread");
        final boolean simple = request.getParameterMap().containsKey("simple");
        final Integer width = getInteger(request, "width", null);
        final Integer height = getInteger(request, "height", null);
        final Double minwidth = getMinWidth(request);
        final boolean reverse = request.getParameterMap().containsKey("reverse");
        out.println("Profiled PID: " + pid);
        out.println("Given duration : " + duration);
        if (process == null || !process.isAlive()) {
            try {
                int lockTimeoutSecs = 3;
                if (profilerLock.tryLock(lockTimeoutSecs, TimeUnit.SECONDS)) {
                    try {
                        out.println("Output_dir: " + OUTPUT_DIR);
                        File outputFile = new File(OUTPUT_DIR, "async-prof-pid-" + pid + "-" +
                                event.name().toLowerCase() + "-" + ID_GEN.incrementAndGet() + "." +
                                output.name().toLowerCase());
                        List<String> cmd = new ArrayList<>();
                        cmd.add(asyncProfilerHome + PROFILER_SCRIPT);
                        cmd.add("-e");
                        cmd.add(event.getInternalName());
                        cmd.add("-d");
                        cmd.add("" + duration);
                        cmd.add("-o");
                        cmd.add(output.name().toLowerCase());
                        cmd.add("-f");
                        cmd.add(outputFile.getAbsolutePath());
                        if (interval != null) {
                            cmd.add("-i");
                            cmd.add(interval.toString());
                        }
                        if (jstackDepth != null) {
                            cmd.add("-j");
                            cmd.add(jstackDepth.toString());
                        }
                        if (bufsize != null) {
                            cmd.add("-b");
                            cmd.add(bufsize.toString());
                        }
                        if (thread) {
                            cmd.add("-t");
                        }
                        if (simple) {
                            cmd.add("-s");
                        }
                        if (width != null) {
                            cmd.add("--width");
                            cmd.add(width.toString());
                        }
                        if (height != null) {
                            cmd.add("--height");
                            cmd.add(height.toString());
                        }
                        if (minwidth != null) {
                            cmd.add("--minwidth");
                            cmd.add(minwidth.toString());
                        }
                        if (reverse) {
                            cmd.add("--reverse");
                        }
                        cmd.add(pid.toString());
                        outputFile.getParentFile().mkdirs(); // create necessary parent (output) folder if it does not exist
                        out.println("request.getPathInfo: " + request.getPathInfo());
                        out.println("Printing servlet path:");
                        out.println(getServletContext().getRealPath(request.getPathInfo()));
                        out.println();
                        process = new ProcessBuilder(cmd).start();

                        // set response and set refresh header to output location
                        setResponseHeader(response);
                        response.setStatus(HttpServletResponse.SC_ACCEPTED);
                        String relativeUrl = "/prof-output";
//                        String relativeUrl = "/prof-output/" + outputFile.getName();
                        out.println("Started [" + event.getInternalName() + "] profiling. This page will automatically redirect to " +
                                relativeUrl + " after " + duration + " seconds.\n\ncommand:\n" + Joiner.on(" ").join(cmd));
                        // to avoid auto-refresh by ProfileOutputServlet, refreshDelay can be specified via url param
                        int refreshDelay = getInteger(request, "refreshDelay", 0);
                        // instead of sending redirect, set auto-refresh so that browsers will refresh with redirected url
                        response.setHeader("Refresh", (duration + refreshDelay) + "; URL=" + relativeUrl + "?file=" + outputFile.getName());
//                        response.setHeader("Refresh", (duration + refreshDelay) + ";" + relativeUrl);
                        out.println("Refreshing to: " + relativeUrl);
                        out.println("Status :" + response.getStatus());
//                        out.flush(); // errors are not displayed because flushing overrides them

                        // write garbage to a file to the path of the expected svg output just to check folders correct
                        Files.write(ARRAY, outputFile);
                    } finally {
                        profilerLock.unlock();
                    }
                } else {
                    setResponseHeader(response);
                    response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
                    out.println("Unable to acquire lock. Another instance of profiler might be running.");
                    LOG.warn("Unable to acquire lock in {} seconds. Another instance of profiler might be running.",
                            lockTimeoutSecs);
                }
            } catch (InterruptedException e) {
                LOG.warn("Interrupted while acquiring profile lock.", e);
                response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            }
        } else {
            setResponseHeader(response);
            response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            out.println("Another instance of profiler is already running.");
        }
        out.close();
    }

    private Integer getInteger(final HttpServletRequest req, final String param, final Integer defaultValue) {
        final String value = req.getParameter(param);
        if (value != null) {
            try {
                return Integer.valueOf(value);
            } catch (NumberFormatException e) {
                return defaultValue;
            }
        }
        return defaultValue;
    }

    private Long getLong(final HttpServletRequest req, final String param) {
        final String value = req.getParameter(param);
        if (value != null) {
            try {
                return Long.valueOf(value);
            } catch (NumberFormatException e) {
                return null;
            }
        }
        return null;
    }

    private Double getMinWidth(final HttpServletRequest req) {
        final String value = req.getParameter("minwidth");
        if (value != null) {
            try {
                return Double.valueOf(value);
            } catch (NumberFormatException e) {
                return null;
            }
        }
        return null;
    }

    private Event getEvent(final HttpServletRequest req) {
        final String eventArg = req.getParameter("event");
        if (eventArg != null) {
            Event event = Event.fromInternalName(eventArg);
            return event == null ? Event.CPU : event;
        }
        return Event.CPU;
    }

    private Output getOutput(final HttpServletRequest req) {
        final String outputArg = req.getParameter("output");
        if (req.getParameter("output") != null) {
            try {
                return Output.valueOf(outputArg.trim().toUpperCase());
            } catch (IllegalArgumentException e) {
                return Output.SVG;
            }
        }
        return Output.SVG;
    }

    private void setResponseHeader(final HttpServletResponse response) {
        response.setHeader(ACCESS_CONTROL_ALLOW_METHODS, ALLOWED_METHODS);
        response.setHeader(ACCESS_CONTROL_ALLOW_ORIGIN, "*");
        response.setContentType(CONTENT_TYPE_TEXT);
    }

    public static String getAsyncProfilerHome() {
        String asyncProfilerHome = System.getenv(ASYNC_PROFILER_HOME_ENV);
        // if ENV is not set, see if -Dasync.profiler.home=/path/to/async/profiler/home is set
        if (asyncProfilerHome == null || asyncProfilerHome.trim().isEmpty()) {
            asyncProfilerHome = System.getProperty(ASYNC_PROFILER_HOME_SYSTEM_PROPERTY);
        }

        return asyncProfilerHome;
    }
}