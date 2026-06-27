/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.parquet.hadoop;

import static org.apache.parquet.format.Util.readFileMetaData;
import static org.apache.parquet.format.Util.writeFileMetaData;
import static org.apache.parquet.schema.MessageTypeParser.parseMessageType;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.nio.file.Files;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.format.LogicalType;
import org.apache.parquet.format.SchemaElement;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TField;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TStruct;
import org.apache.thrift.protocol.TType;
import org.apache.thrift.transport.TIOStreamTransport;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestReadWithFutureLogicalType {
  private static final short FUTURE_LOGICAL_TYPE_FIELD_ID = Short.MAX_VALUE;
  private static final String FUTURE_LOGICAL_TYPE_FIELD_NAME = "FUTURE";
  private static final String FUTURE_FIELD_NAME = "future";
  private static final MessageType FILE_SCHEMA = parseMessageType("message root {\n"
      + "  required int32 id = 1;\n"
      + "  optional binary future = 2;\n"
      + "}");
  private static final String ID_ONLY_PROJECTION_SCHEMA = "message root {\n" + "  required int32 id = 1;\n" + "}";

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  @Test
  public void testReadProjectedColumnFromFileWithFutureLogicalType() throws Exception {
    Path file = createFileWithFutureLogicalType();

    try (ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), file)
        // Request only the known id column; the unknown logical type column is intentionally not projected.
        .set(ReadSupport.PARQUET_READ_SCHEMA, ID_ONLY_PROJECTION_SCHEMA)
        .build()) {
      Group row = reader.read();
      assertNotNull(row);
      assertEquals(1, row.getInteger("id", 0));
      assertNull(reader.read());
    }
  }

  @Test
  public void testFutureLogicalTypeIsIgnoredInFooterSchema() throws Exception {
    Path file = createFileWithFutureLogicalType();

    ParquetMetadata footer =
        ParquetFileReader.readFooter(new Configuration(), file, ParquetMetadataConverter.NO_FILTER);

    assertNull(footer.getFileMetaData()
        .getSchema()
        .getType(FUTURE_FIELD_NAME)
        .getLogicalTypeAnnotation());
    assertEquals(
        PrimitiveTypeName.BINARY,
        footer.getFileMetaData()
            .getSchema()
            .getType(FUTURE_FIELD_NAME)
            .asPrimitiveType()
            .getPrimitiveTypeName());
  }

  @Test
  public void testReadFutureLogicalTypeColumnAsPhysicalSchema() throws Exception {
    Path file = createFileWithFutureLogicalType();

    try (ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), file).build()) {
      Group row = reader.read();
      assertNotNull(row);
      assertEquals(1, row.getInteger("id", 0));
      assertEquals(1, row.getFieldRepetitionCount(FUTURE_FIELD_NAME));
      assertEquals("payload", row.getBinary(FUTURE_FIELD_NAME, 0).toStringUsingUTF8());
      assertNull(reader.read());
    }
  }

  @Test
  public void testFutureLogicalTypeUnionFieldHasNoSetField() throws Exception {
    LogicalType logicalType = new LogicalType();
    logicalType.read(new TCompactProtocol(new TIOStreamTransport(new ByteArrayInputStream(futureLogicalTypeBytes()))));

    assertNull(logicalType.getSetField());
  }

  private Path createFileWithFutureLogicalType() throws Exception {
    File source = new File(temp.getRoot(), "source.parquet");
    File target = new File(temp.getRoot(), "future-logical-type.parquet");

    writeBaseFile(new Path(source.toURI()));
    rewriteFooterWithFutureLogicalType(source, target);
    return new Path(target.toURI());
  }

  private static void writeBaseFile(Path file) throws Exception {
    SimpleGroupFactory factory = new SimpleGroupFactory(FILE_SCHEMA);
    try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(file).withType(FILE_SCHEMA).build()) {
      writer.write(factory.newGroup()
          .append("id", 1)
          .append(FUTURE_FIELD_NAME, Binary.fromString("payload")));
    }
  }

  private static void rewriteFooterWithFutureLogicalType(File source, File target) throws Exception {
    // Keep the data pages produced by parquet-java and mutate only the footer schema annotation.
    byte[] fileBytes = Files.readAllBytes(source.toPath());
    int footerLength =
        BytesUtils.readIntLittleEndian(fileBytes, fileBytes.length - ParquetFileWriter.MAGIC.length - 4);
    int footerIndex = fileBytes.length - ParquetFileWriter.MAGIC.length - 4 - footerLength;

    org.apache.parquet.format.FileMetaData footer =
        readFileMetaData(new ByteArrayInputStream(fileBytes, footerIndex, footerLength));
    setFutureLogicalType(footer);

    ByteArrayOutputStream footerBytes = new ByteArrayOutputStream();
    writeFileMetaData(footer, footerBytes);

    ByteArrayOutputStream rewrittenFile = new ByteArrayOutputStream();
    rewrittenFile.write(fileBytes, 0, footerIndex);
    footerBytes.writeTo(rewrittenFile);
    BytesUtils.writeIntLittleEndian(rewrittenFile, footerBytes.size());
    rewrittenFile.write(ParquetFileWriter.MAGIC);
    Files.write(target.toPath(), rewrittenFile.toByteArray());
  }

  private static void setFutureLogicalType(org.apache.parquet.format.FileMetaData footer) {
    for (SchemaElement schemaElement : footer.getSchema()) {
      if (FUTURE_FIELD_NAME.equals(schemaElement.getName())) {
        schemaElement.setLogicalType(new FutureLogicalType());
        return;
      }
    }
    throw new AssertionError("Could not find field " + FUTURE_FIELD_NAME);
  }

  private static byte[] futureLogicalTypeBytes() throws Exception {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    new FutureLogicalType().write(new TCompactProtocol(new TIOStreamTransport(out)));
    return out.toByteArray();
  }

  private static class FutureLogicalType extends LogicalType {
    private static final TStruct LOGICAL_TYPE_STRUCT = new TStruct("LogicalType");
    private static final TStruct FUTURE_TYPE_STRUCT = new TStruct(FUTURE_LOGICAL_TYPE_FIELD_NAME);
    private static final TField FUTURE_TYPE_FIELD =
        new TField(FUTURE_LOGICAL_TYPE_FIELD_NAME, TType.STRUCT, FUTURE_LOGICAL_TYPE_FIELD_ID);

    @Override
    public void write(TProtocol protocol) throws TException {
      protocol.writeStructBegin(LOGICAL_TYPE_STRUCT);
      protocol.writeFieldBegin(FUTURE_TYPE_FIELD);
      protocol.writeStructBegin(FUTURE_TYPE_STRUCT);
      protocol.writeFieldStop();
      protocol.writeStructEnd();
      protocol.writeFieldEnd();
      protocol.writeFieldStop();
      protocol.writeStructEnd();
    }
  }
}
