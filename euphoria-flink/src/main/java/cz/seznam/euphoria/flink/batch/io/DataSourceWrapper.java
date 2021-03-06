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
package cz.seznam.euphoria.flink.batch.io;

import cz.seznam.euphoria.shadow.com.google.common.base.Preconditions;
import cz.seznam.euphoria.core.client.dataset.windowing.GlobalWindowing;
import cz.seznam.euphoria.core.client.io.BoundedDataSource;
import cz.seznam.euphoria.core.client.io.BoundedReader;
import cz.seznam.euphoria.flink.batch.BatchElement;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.core.io.LocatableInputSplit;

import java.io.IOException;
import java.util.List;
import java.util.function.BiFunction;

public class DataSourceWrapper<T>
    implements InputFormat<BatchElement<GlobalWindowing.Window, T>,
    PartitionWrapper<T>>,
    ResultTypeQueryable<T> {

  private final BoundedDataSource<T> dataSource;
  private final BiFunction<LocatableInputSplit[], Integer, InputSplitAssigner> splitAssignerFactory;

  /** currently opened reader (if any) */
  private transient BoundedReader<T> reader;

  private final List<BoundedDataSource<T>> splits;

  public DataSourceWrapper(
      BoundedDataSource<T> dataSource,
      BiFunction<LocatableInputSplit[], Integer,
          InputSplitAssigner> splitAssignerFactory,
      int parallel) {

    Preconditions.checkArgument(dataSource.isBounded());
    this.dataSource = dataSource;
    this.splitAssignerFactory = splitAssignerFactory;
    long sizeEstimate = dataSource.sizeEstimate();
    splits = dataSource.split(sizeEstimate / parallel);
  }

  @Override
  public void configure(Configuration parameters) {
    // ignore configuration
  }

  @Override
  public BaseStatistics getStatistics(BaseStatistics cachedStatistics) throws IOException {
    // TODO euphoria cannot provide the stats at the moment determine whether it is
    // worth building an abstraction such that we could provide the stats to flink
    return null;
  }

  @Override
  public PartitionWrapper<T>[] createInputSplits(int minNumSplits) throws IOException {
    if (minNumSplits > splits.size()) {
      throw new IllegalArgumentException(
          "minNumSplits must be less or equals to splits size");
    }
    @SuppressWarnings("unchecked")
    PartitionWrapper<T>[] splitWrappers = new PartitionWrapper[splits.size()];
    for (int i = 0; i < splits.size(); i++) {
      splitWrappers[i] = new PartitionWrapper<>(i, splits.get(i));
    }

    return splitWrappers;
  }

  @Override
  public InputSplitAssigner getInputSplitAssigner(
      PartitionWrapper[] partitions) {

    return splitAssignerFactory.apply(partitions, partitions.length);
  }

  @Override
  public void open(PartitionWrapper<T> partition) throws IOException {
    this.reader = partition.getSource().openReader();
  }

  @Override
  public boolean reachedEnd() throws IOException {
    return !reader.hasNext();
  }

  @Override
  public BatchElement<GlobalWindowing.Window, T> nextRecord(
          BatchElement<GlobalWindowing.Window, T> reuse)
      throws IOException {
    return new BatchElement<>(GlobalWindowing.Window.get(), 0L, reader.next());
  }

  @Override
  public void close() throws IOException {
    reader.close();
  }

  @Override
  @SuppressWarnings("unchecked")
  public TypeInformation<T> getProducedType() {
    return TypeInformation.of((Class) BatchElement.class);
  }

  public int getParallelism() {
    return splits.size();
  }

}
