package com.datasalt.pangool.hive;

import com.datasalt.pangool.tuplemr.mapred.lib.input.TupleFileRecordReader;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

public class PangoolHiveInputFormat  extends FileInputFormat<NullWritable, TupleWritable>  {
  @Override
  public RecordReader<NullWritable, TupleWritable> createRecordReader(final InputSplit inputSplit, final TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
    return new RecordReader<NullWritable, TupleWritable>() {
      TupleFileRecordReader innerReader = new TupleFileRecordReader();
      TupleWritable tupleWrapper = new TupleWritable();

      @Override
      public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        innerReader.initialize(inputSplit, taskAttemptContext);
      }

      @Override
      public boolean nextKeyValue() throws IOException, InterruptedException {
        return innerReader.nextKeyValue();
      }

      @Override
      public NullWritable getCurrentKey() throws IOException, InterruptedException {
        return NullWritable.get();
      }

      @Override
      public TupleWritable getCurrentValue() throws IOException, InterruptedException {
        tupleWrapper.set(innerReader.getCurrentKey());
        return tupleWrapper;
      }

      @Override
      public float getProgress() throws IOException, InterruptedException {
        return innerReader.getProgress();
      }

      @Override
      public void close() throws IOException {
        innerReader.close();
      }
    };
  }
}
