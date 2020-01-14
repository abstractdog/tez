package org.apache.tez.history;

import java.io.File;

import org.apache.hadoop.fs.Path;
import org.apache.tez.history.parser.HistoryEventProtoParser;
import org.junit.Test;

public class TestHistoryEventProtoParser {

  @Test
  public void testProtoParser() throws Exception {
    HistoryEventProtoParser parser = new HistoryEventProtoParser(
        new Path(new File("/home/abstractdog/Downloads/tez/sys.db/dag_data/date=2019-11-30/dag_1575066054623_0001_1_1").toURI()));
    parser.getDAGData();
  }
}
