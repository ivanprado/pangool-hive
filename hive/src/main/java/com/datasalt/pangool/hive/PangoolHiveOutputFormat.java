package com.datasalt.pangool.hive;

import com.datasalt.pangool.io.Schema;
import com.datasalt.pangool.io.TupleFile;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;
import java.util.Properties;

public class PangoolHiveOutputFormat implements HiveOutputFormat<LongWritable, TupleWritable> {

  @Override
  public FileSinkOperator.RecordWriter getHiveRecordWriter(final JobConf conf, final Path path,
                                                           final Class<? extends Writable> aClass,
                                                           final boolean isCompressed, final Properties properties,
                                                           final Progressable progressable) throws IOException {
    return new FileSinkOperator.RecordWriter() {
      TupleFile.Writer writer = null;


      public void initWriter(Schema schema) throws IOException {


        CompressionCodec codec = null;
        SequenceFile.CompressionType compressionType = SequenceFile.CompressionType.NONE;
        if (isCompressed) {
          // find the kind of compression to do
          compressionType = SequenceFileOutputFormat.getOutputCompressionType(conf);

          // find the right codec
          Class<?> codecClass = SequenceFileOutputFormat.getOutputCompressorClass(conf,
              DefaultCodec.class);
          codec = (CompressionCodec)
              ReflectionUtils.newInstance(codecClass, conf);
        }

        new Configuration();

        // get the path of the temporary output file
        FileSystem fs = path.getFileSystem(conf);
        writer =
            new TupleFile.Writer(fs, conf, path, schema,
                compressionType,
                codec,
                progressable);

      }

      @Override
      public void write(Writable writable) throws IOException {
        TupleWritable tw = (TupleWritable) writable;
        writer.append(tw.get());
      }

      @Override
      public void close(boolean b) throws IOException {
        if (writer != null) {
          writer.close();
        }
      }
    };
  }
}
