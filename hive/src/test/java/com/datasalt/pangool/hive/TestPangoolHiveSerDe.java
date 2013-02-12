/**
 * Copyright [2012] [Datasalt Systems S.L.]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datasalt.pangool.hive;

import com.datasalt.pangool.bootstrap.SortJob;
import com.datasalt.pangool.io.ITuple;
import com.datasalt.pangool.io.Schema;
import com.datasalt.pangool.io.Tuple;
import com.datasalt.pangool.io.TupleFile;
import com.google.common.io.Files;
import junit.framework.TestCase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Test;

import java.io.File;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;

import static org.junit.Assert.*;
import static org.junit.Assert.fail;

/**
 * Test class for {@link com.datasalt.pangool.bootstrap.SortJob}
 */
public class TestPangoolHiveSerDe extends TestCase {

  private static final String driverName = "org.apache.hadoop.hive.jdbc.HiveDriver";
  private final HiveConf conf;
  private final Path dataFilePath;
  private final Path dataTypeDataFilePath;
  private final boolean standAloneServer;

  public TestPangoolHiveSerDe(String name) {
    super(name);
    conf = new HiveConf(TestPangoolHiveSerDe.class);
    String dataFileDir = conf.get("test.data.files").replace('\\', '/')
        .replace("c:", "");
    dataFilePath = new Path(dataFileDir, "kv1.txt");
    dataTypeDataFilePath = new Path(dataFileDir, "datatypes.txt");
    standAloneServer = "true".equals(System
        .getProperty("test.service.standalone.server"));
  }


  static Schema schema;

  enum TestEnum { ENUM0, ENUM1 };

  static {
    ArrayList<Schema.Field> fields = new ArrayList<Schema.Field>();
    fields.add(Schema.Field.create("cint", Schema.Field.Type.INT, true));
    fields.add(Schema.Field.create("clong", Schema.Field.Type.LONG, true));
    fields.add(Schema.Field.create("cfloat", Schema.Field.Type.FLOAT, true));
    fields.add(Schema.Field.create("cdouble", Schema.Field.Type.DOUBLE, true));
    fields.add(Schema.Field.create("cstring", Schema.Field.Type.STRING, true));
    fields.add(Schema.Field.create("cboolean", Schema.Field.Type.BOOLEAN, true));
    fields.add(Schema.Field.create("cenum", Schema.Field.Type.ENUM, true));
    fields.add(Schema.Field.create("cbytes", Schema.Field.Type.BYTES, true));
    schema = new Schema("testSchema", fields);
  }

  public ITuple fillTuple(Random rand, ITuple t) {
    Schema schema = t.getSchema();
    for (int i = 0; i < schema.getFields().size(); i++) {
      Object value = null;

      // 30% of null values. 70% of non null ones.
      if (rand.nextDouble() > 0.30d) {
        Schema.Field.Type type = schema.getField(i).getType();
        switch (type) {
          case INT:
            value = rand.nextInt();
            break;
          case LONG:
            value = rand.nextLong();
            break;
          case FLOAT:
            value = rand.nextFloat();
            break;
          case DOUBLE:
            value = rand.nextDouble();
            break;
          case STRING:
            value = "str" + rand.nextDouble();
            break;
          case BOOLEAN:
            value = rand.nextDouble() < 0.5d;
            break;
          case ENUM:
            value = (rand.nextDouble() < 0.5d) ? TestEnum.ENUM0 : TestEnum.ENUM1;
            break;
          case BYTES:
            int size = rand.nextInt() % 3;
            byte[] arr = new byte[size];
            for (int j=0; j<size; j++) {
              arr[j] = (byte) rand.nextInt();
            }
            value = arr;
            break;
          case OBJECT:
            value = new IntWritable(rand.nextInt());
            break;
        }
      }

      t.set(i, value);
    }
    return t;
  }

  void assertEqualTuples(ITuple t1, ITuple t2) {
    Schema schema = t1.getSchema();

    String msg = t1 + " vs " + t2;

    for (int i = 0; i < schema.getFields().size(); i++) {
      Schema.Field field = schema.getField(i);
      msg = "Field: " + field.getName() + " - " + msg;
      Object value1 = t1.get(i);
      Object value2 = t2.get(i);
      if (value1 == null) {
        assertNull(msg, value2);
      } else if (value1 instanceof byte[]) {
        assertTrue(msg, Arrays.equals((byte[]) value1, (byte[]) value2));
      } else {
        assertEquals(msg, value1, value2);
      }
      // not same instance
      if (field.getType() == Schema.Field.Type.OBJECT || field.getType() == Schema.Field.Type.BYTES) {
        assertTrue(msg, t1.get(i) != t2.get(i) || (t1.get(i) == null && t2.get(i) == null));
      }
    }
  }

  @Test
  public void test() throws Exception {

    String INPUT = getClass().getCanonicalName() + "-input";
    String OUTPUT = getClass().getCanonicalName() + "-output";
    int NUM_ROWS = 50;

    Configuration hConf = new Configuration();
    FileSystem fs = FileSystem.get(hConf);

    fs.delete(new Path(INPUT), true);
    fs.delete(new Path(OUTPUT), true);

    Class.forName(driverName);
    Connection con;
    if (standAloneServer) {
      // get connection
      con = DriverManager.getConnection("jdbc:hive://localhost:10000/default",
          "", "");
    } else {
      con = DriverManager.getConnection("jdbc:hive://", "", "");
    }

    // Writing a file with tuples
    TupleFile.Writer writer = new TupleFile.Writer(fs, hConf, new Path(INPUT), schema);
    Random rand = new Random(1);
    Tuple tuple = new Tuple(schema);

    for (int i = 0; i < NUM_ROWS; i++) {
      writer.append(fillTuple(rand, tuple));
    }

    writer.close();

    Statement stmt = con.createStatement();
    try {
      stmt.executeQuery("drop table pangool_test");
    } catch (Exception e) {
      // Do nothing. Probably the table don't exist.
    }


    String create_table = "create external table TABLENAME (" +
        "cint int, clong bigint, cfloat float, cdouble double, cstring string, cboolean boolean, " +
        "cenum int, cbytes binary " +
        "ROW FORMAT SERDE 'com.datasalt.pangool.hive.PangoolHiveSerDe' " +
        "STORED AS INPUTFORMAT 'com.datasalt.pangool.hive.PangoolHiveInputFormat' " +
        "OUTPUTFORMAT 'com.datasalt.pangool.hive.PangoolHiveoOutputFormat' " +
        "TBLPROPERTIES ('schema.name'='myschema' ";


    String create_table1 = create_table.replace("TABLENAME", "table1") + "LOCATION '" + INPUT + "');";
    String create_table2 = create_table.replace("TABLENAME", "table2") + "LOCATION '" + OUTPUT + "');";

    stmt.executeQuery();

    stmt.executeQuery("load data inpath ");

    TupleFile.Reader readerSource = new TupleFile.Reader(fs, hConf, new Path(INPUT));
    TupleFile.Reader readerTarget = new TupleFile.Reader(fs, hConf, new Path(OUTPUT));

    ITuple sourceTuple = null;
    ITuple targetTuple = null;
    while(readerSource.next(sourceTuple)) {
      readerTarget.next(targetTuple);
      assertEqualTuples(sourceTuple, targetTuple);
    }
    assertFalse(readerTarget.next(tuple));

    readerSource.close();
    readerTarget.close();

    stmt.executeQuery("drop table pangool_test");
    stmt.close();
    con.close();

    fs.delete(new Path(INPUT), true);
    fs.delete(new Path(OUTPUT), true);
  }
}
