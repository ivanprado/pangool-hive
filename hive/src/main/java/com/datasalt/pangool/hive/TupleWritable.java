package com.datasalt.pangool.hive;

import com.datasalt.pangool.io.ITuple;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * A class for wrapping an {@link ITuple}
 */
public class TupleWritable implements Writable {

  ITuple wrappedTuple;

  public TupleWritable() {  }

  public void set(ITuple tuple) {
    wrappedTuple = tuple;
  }

  public ITuple get() {
    return wrappedTuple;
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    throw new UnsupportedOperationException("This class implements Writable just to follow the " +
        "Hive Serde API. But it does not support the Writable API for serializing and deserializing.");
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    throw new UnsupportedOperationException("This class implements Writable just to follow the " +
        "Hive Serde API. But it does not support the Writable API for serializing and deserializing.");
  }
}
