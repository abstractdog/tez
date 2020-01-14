package org.apache.tez.dag.history.logging.proto;

import java.time.LocalDate;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.SystemClock;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.history.HistoryEvent;
import org.apache.tez.dag.history.HistoryEventType;
import org.apache.tez.dag.history.logging.proto.HistoryLoggerProtos.HistoryEventProto;
import org.apache.tez.dag.records.TezDAGID;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestTezProtoLoggers {
  private static final String LOG_DIR = "src/test/resources/sys.db";

  private static ApplicationId appId = ApplicationId.newInstance(1575066054623l, 1);
  private static TezDAGID dagId = TezDAGID.getInstance(appId, 1);

  private static DatePartitionedLogger<HistoryEventProto> dagEventsLogger;

  private static class FixedClock implements Clock {
    final Clock clock = SystemClock.getInstance();
    final long diff;

    public FixedClock(long startTime) {
      diff = clock.getTime() - startTime;
    }

    @Override
    public long getTime() {
      return clock.getTime() - diff;
    }
  }

  @BeforeClass
  public static void beforeClass() throws Exception {
    TezConfiguration conf = new TezConfiguration();
    Clock clock = new FixedClock(System.currentTimeMillis());
    conf.set(TezConfiguration.TEZ_HISTORY_LOGGING_PROTO_BASE_DIR, LOG_DIR);

    dagEventsLogger = new DatePartitionedLogger<>(HistoryEventProto.PARSER, new Path(LOG_DIR, "dag_data"), conf, clock);
  }

  @Test
  public void testParseDagEvent() throws Exception {
    Path dagFilePath = dagEventsLogger.getPathForDate(LocalDate.parse("2019-11-30"), dagId + "_1");
    ProtoMessageReader<HistoryEventProto> appReader = dagEventsLogger.getReader(dagFilePath);
    HistoryEventProto historyEventProto = appReader.readEvent();

    while (historyEventProto != null) {
      System.out.println();

      HistoryEventType eventType = HistoryEventType.valueOf(historyEventProto.getEventType());
      //HistoryEvent event = HistoryEventType.createHistoryEventFromType(eventType);
 
      //historyEventProto = appReader.readEvent();
      //System.out.println(event.getEventType());
    }

    appReader.close();
  }
}
