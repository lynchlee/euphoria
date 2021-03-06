/*
 * Copyright 2016-2018 Seznam.cz, a.s.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cz.seznam.euphoria.hadoop.output;

import cz.seznam.euphoria.core.client.io.DataSink;
import cz.seznam.euphoria.core.client.io.Writer;

import cz.seznam.euphoria.hadoop.utils.Serializer;
import java.io.IOException;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

/**
 * {@link OutputFormat} created from {@link DataSink}.
 * Because of the hadoop output format contract, we need to be able to
 * instantiate the format from {@link Class} object, therefore we
 * need to serialize the underlying {@link DataSink} to bytes and
 * store in to configuration.
 *
 * @param <V> the type of the elements written through this output format
 */
public class DataSinkOutputFormat<V> extends OutputFormat<NullWritable, V> {

  private static final String DATA_SINK = "cz.seznam.euphoria.hadoop.data-sink-serialized";

  /**
   * Sets/Serializes given {@link DataSink} into Hadoop configuration. Note that
   * original configuration is modified.
   *
   * @param conf Instance of Hadoop configuration
   * @param sink Euphoria sink
   *
   * @return Modified configuration
   *
   * @throws IOException if serializing the given data sink fails for some reason
   */
  public static Configuration configure(Configuration conf, DataSink<?> sink) throws IOException {
    conf.set(DATA_SINK, toBase64(sink));
    return conf;
  }

  private static class HadoopRecordWriter<V> extends RecordWriter<NullWritable, V> {

    private final Writer<V> writer;

    HadoopRecordWriter(Writer<V> writer) {
      this.writer = writer;
    }

    @Override
    public void write(NullWritable k, V v) throws IOException {
      writer.write(v);
    }

    @Override
    public void close(TaskAttemptContext tac) throws IOException {
      writer.flush();
    }

  }

  private static String toBase64(DataSink<?> sink) throws IOException {
    byte[] bytes = Serializer.toBytes(sink);
    return Base64.getEncoder().encodeToString(bytes);
  }

  private static <V> DataSink<V> fromBase64(String base64Bytes)
      throws IOException, ClassNotFoundException {
    byte[] bytes = Base64.getDecoder().decode(base64Bytes);
    return Serializer.fromBytes(bytes);
  }

  @Nullable
  @GuardedBy("lock")
  private DataSink<V> sink;

  @GuardedBy("lock")
  private Map<TaskAttemptID, Writer<V>> writers = new HashMap<>();

  private final Object lock = new Object();

  @Override
  public RecordWriter<NullWritable, V> getRecordWriter(TaskAttemptContext tac)
      throws IOException, InterruptedException {
    return new HadoopRecordWriter<>(getWriter(tac));
  }

  @Override
  public void checkOutputSpecs(JobContext jc) throws IOException, InterruptedException {

  }

  @Override
  public OutputCommitter getOutputCommitter(TaskAttemptContext tac)
      throws IOException, InterruptedException {
    
    return new OutputCommitter() {

      private Writer<V> innerWriter;

      @Override
      public void setupJob(JobContext jc) throws IOException {
        getSink(jc).initialize();
      }

      @Override
      public void setupTask(TaskAttemptContext tac) throws IOException {
        innerWriter = getWriter(tac);
      }

      @Override
      public boolean needsTaskCommit(TaskAttemptContext tac) throws IOException {
        return true;
      }

      @Override
      public void commitTask(TaskAttemptContext tac) throws IOException {
        if (innerWriter != null) {
          innerWriter.commit();
          innerWriter.close();
        }
      }

      @Override
      public void abortTask(TaskAttemptContext tac) throws IOException {
        if (innerWriter != null) {
          innerWriter.rollback();
          innerWriter.close();
        }
      }

      @Override
      public void commitJob(JobContext jobContext) throws IOException {
        super.commitJob(jobContext);
        getSink(jobContext).commit();
      }

      @Override
      public void abortJob(JobContext jobContext, JobStatus.State state)
          throws IOException {        
        super.abortJob(jobContext, state);
        getSink(jobContext).rollback();
      }

    };
  }

  private Writer<V> getWriter(TaskAttemptContext tac) throws IOException {
    synchronized (lock) {
      final TaskAttemptID tai = tac.getTaskAttemptID();
      if (!writers.containsKey(tai)) {
        writers.put(tai, getSink(tac).openWriter(tai.getTaskID().getId()));
      }
      return writers.get(tai);
    }
  }

  private DataSink<V> getSink(JobContext jc) throws IOException {
    synchronized (lock) {
      if (sink == null) {
        String sinkBytes = jc.getConfiguration().get(DATA_SINK, null);
        if (sinkBytes == null) {
          throw new IllegalStateException(
              "Invalid output spec, call `DataSinkOutputFormat#configure` before passing "
                  + " the configuration to output");
        }
        try {
          sink = fromBase64(sinkBytes);
        } catch (ClassNotFoundException ex) {
          throw new IOException(ex);
        }
      }
      return sink;
    }
  }

}
