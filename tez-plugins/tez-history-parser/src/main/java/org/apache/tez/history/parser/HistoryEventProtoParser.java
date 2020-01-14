package org.apache.tez.history.parser;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.history.HistoryEvent;
import org.apache.tez.dag.history.HistoryEventType;
import org.apache.tez.dag.history.events.DAGSubmittedEvent;
import org.apache.tez.dag.history.logging.impl.HistoryEventJsonConversion;
import org.apache.tez.dag.history.logging.proto.HistoryLoggerProtos.HistoryEventProto;
import org.apache.tez.dag.history.logging.proto.ProtoMessageReader;
import org.apache.tez.dag.recovery.records.RecoveryProtos.DAGSubmittedProto;
import org.apache.tez.history.parser.datamodel.BaseParser;
import org.apache.tez.history.parser.datamodel.DagInfo;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;

public class HistoryEventProtoParser extends BaseParser {
  private static final Logger LOG = LoggerFactory.getLogger(HistoryEventProtoParser.class);
  private final ProtoMessageReader<HistoryEventProto> reader;
  private TezConfiguration conf;
  private Path protoFile;

  public HistoryEventProtoParser(Path protoFile) throws IOException {
    super();
    this.conf = new TezConfiguration();
    this.protoFile = protoFile;
    this.reader =
        new ProtoMessageReader<HistoryEventProto>(conf, protoFile, HistoryEventProto.PARSER);
  }

  public DagInfo getDAGData() throws Exception {
    parseContents();
    return dagInfo;
  }

  private void parseContents() throws Exception {
    HistoryEventProto historyEventProto = reader.readEvent();

    while (historyEventProto != null) {
      HistoryEventType eventType = HistoryEventType.valueOf(historyEventProto.getEventType());
      HistoryEvent event = HistoryEventType.createHistoryEventFromType(eventType);

      System.out.println(event.getEventType());
      System.out.println(historyEventProto.getDagId());
      System.out.println(historyEventProto.getUser());

      ByteArrayOutputStream os = new ByteArrayOutputStream();
      //CodedOutputStream codedOutputStream = CodedOutputStream.newInstance(os);
      //historyEventProto.writeTo(codedOutputStream);
      historyEventProto.writeTo(os);
      //os.flush();
      //os.close();
      //codedOutputStream.flush();
      System.out.println(os.toByteArray().length);
      InputStream is = new ByteArrayInputStream(os.toByteArray());
      CodedInputStream stream = CodedInputStream.newInstance(is);

      event.fromProtoStream(stream);

      JSONObject eventJson = HistoryEventJsonConversion.convertToJson(event);

      try {
        historyEventProto = reader.readEvent();
      } catch (EOFException e) {
        break;
      }
    }
    
    /*
    FileSystem fs = FileSystem.get(conf);
    CodedInputStream codedInputStream = CodedInputStream.newInstance(fs.open(this.protoFile));

    codedInputStream.setSizeLimit(Integer.MAX_VALUE);
    while (true) {
      HistoryEvent historyEvent = getNextEvent(codedInputStream);
      if (historyEvent == null) {
        LOG.info("Reached end of stream");
        break;
      }
      LOG.debug("Read HistoryEvent, eventType={}, event={}", historyEvent.getEventType(), historyEvent);
    }
    */
  }
  
  private HistoryEvent getNextEvent(CodedInputStream inputStream)
      throws IOException {
    boolean isAtEnd = inputStream.isAtEnd();
    if (isAtEnd) {
      return null;
    }
    int eventTypeOrdinal = -1;
    try {
      eventTypeOrdinal = inputStream.readFixed32();
    } catch (EOFException eof) {
      return null;
    }
    if (eventTypeOrdinal < 0 || eventTypeOrdinal >=
        HistoryEventType.values().length) {
      // Corrupt data
      // reached end
      throw new IOException("Corrupt data found when trying to read next event type"
          + ", eventTypeOrdinal=" + eventTypeOrdinal);
    }
    HistoryEventType eventType = HistoryEventType.values()[eventTypeOrdinal];
    HistoryEvent event = HistoryEventType.createHistoryEventFromType(eventType);

    try {
      event.fromProtoStream(inputStream);
    } catch (EOFException eof) {
      return null;
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Parsed event from input stream"
          + ", eventType=" + eventType
          + ", event=" + event.toString());
    }
    return event;
  }
}
