package org.apache.tez.client;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;

import com.google.protobuf.ByteString;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;
import com.google.protobuf.Parser;
import com.google.protobuf.UnknownFieldSet;

public class MessageWrap implements Message {

  private final Message message;

  public MessageWrap(Message message){
    this.message = message;
  }

  @Override
  public boolean hasField(FieldDescriptor field) {
    return message.hasField(field);
  }
  
  @Override
  public UnknownFieldSet getUnknownFields() {
    return message.getUnknownFields();
  }
  
  @Override
  public int getRepeatedFieldCount(FieldDescriptor field) {
    return message.getRepeatedFieldCount(field);
  }
  
  @Override
  public Object getRepeatedField(FieldDescriptor field, int index) {
    return message.getRepeatedField(field, index);
  }
  
  @Override
  public String getInitializationErrorString() {
    return message.getInitializationErrorString();
  }
  
  @Override
  public Object getField(FieldDescriptor field) {
    return message.getField(field);
  }
  
  @Override
  public Descriptor getDescriptorForType() {
    return message.getDescriptorForType();
  }
  
  @Override
  public Map<FieldDescriptor, Object> getAllFields() {
    // TODO Auto-generated method stub
    return null;
  }
  
  @Override
  public List<String> findInitializationErrors() {
    return message.findInitializationErrors();
  }
  
  @Override
  public boolean isInitialized() {
    return message.isInitialized();
  }
  
  @Override
  public Message getDefaultInstanceForType() {
    return message.getDefaultInstanceForType();
  }
  
  @Override
  public void writeTo(OutputStream output) throws IOException {
    message.writeTo(output);
  }
  
  @Override
  public void writeTo(CodedOutputStream output) throws IOException {
    message.writeTo(output);
  }
  
  @Override
  public void writeDelimitedTo(OutputStream output) throws IOException {
    message.writeDelimitedTo(output);
  }
  
  @Override
  public ByteString toByteString() {
    return message.toByteString();
  }
  
  @Override
  public byte[] toByteArray() {
    return message.toByteArray();
  }
  
  @Override
  public int getSerializedSize() {
    return message.getSerializedSize();
  }
  
  @Override
  public Builder toBuilder() {
    return message.toBuilder();
  }
  
  @Override
  public Builder newBuilderForType() {
    return message.newBuilderForType();
  }
  
  @Override
  public Parser<? extends Message> getParserForType() {
    return message.getParserForType();
  }
}
