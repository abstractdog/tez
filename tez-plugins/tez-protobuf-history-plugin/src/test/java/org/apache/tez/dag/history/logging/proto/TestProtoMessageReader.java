package org.apache.tez.dag.history.logging.proto;

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.Path;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.history.logging.proto.HistoryLoggerProtos.HistoryEventProto;
import org.codehaus.jettison.json.JSONObject;
import org.junit.Test;

public class TestProtoMessageReader {

  @Test
  public void testReader() throws Exception {
    ProtoMessageReader r = new ProtoMessageReader(new TezConfiguration(),
        new Path("src/test/resources/dag_1583980529217_0000_18_1_1"), HistoryEventProto.PARSER);

    HistoryEventProto message = (HistoryEventProto)r.readEvent();
    int index = 0;
    while (message != null){
      index+=1;
      //String eventType = message.getEventType();
      //File folder = new File("src/test/resources/" + eventType);
      //folder.mkdirs();

      JSONObject jsonObject = HistoryEventProtoJsonConversion.convertToJson(message);
      System.out.println("*****" + jsonObject);

      //FileUtils.writeStringToFile(new File(folder, index + ".txt"), message.toString());
      try {
        message = (HistoryEventProto) r.readEvent();
      } catch (java.io.EOFException e) {
        message = null;
      }
    }
    System.out.println("*****" + index);
  }
}
