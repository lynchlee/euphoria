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
package cz.seznam.euphoria.core.executor.io;

import cz.seznam.euphoria.core.annotation.audience.Audience;
import cz.seznam.euphoria.core.client.io.ExternalIterable;
import cz.seznam.euphoria.core.client.operator.state.ListStorage;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;

/**
 * A list storage implementation keeping a given maximum number of received
 * elements, eventually starting to spill them to the local file system.
 * Intended for use in batch executors.
 *
 * @param <T> the type of elements stored in this list storage
 */
@Audience(Audience.Type.EXECUTOR)
public class FsSpillingListStorage<T> implements ListStorage<T>, ExternalIterable<T> {

  private final SerializerFactory serializerFactory;
  private final SpillFileFactory spillFileFactory;
  private final int maxElemsInMemory;

  // ~ new elements are appended to this list and eventually this list
  // will be spilled to disk. therefore, this list contains the lastly
  // added elements to this list-storage
  private final List<T> elems = new ArrayList<>();

  private File storageFile;
  private SerializerFactory.Serializer serializerInstance;
  private SerializerFactory.Serializer.Output serializerStream;
  private boolean needsFlush;

  public FsSpillingListStorage(
      SerializerFactory serializerFactory,
      SpillFileFactory spillFileFactory,
      int maxElemsInMemory) {

    this.serializerFactory = Objects.requireNonNull(serializerFactory);
    this.spillFileFactory = Objects.requireNonNull(spillFileFactory);
    this.maxElemsInMemory = maxElemsInMemory;
  }

  @Override
  public void add(T element) {
    elems.add(element);
    if (elems.size() > maxElemsInMemory) {
      spillElems();
    }
  }

  private void spillElems() {
    if (serializerStream == null) {
      initDiskStorage();
    }
    for (T elem : elems) {
      serializerStream.writeObject(elem);
    }
    elems.clear();
    needsFlush = true;
  }

  @Override
  public Iterable<T> get() {
    return () -> {
      if (serializerStream == null && elems.isEmpty()
          && (storageFile == null || !storageFile.exists())) {

        return Collections.emptyIterator();
      }

      SerializerFactory.Serializer.Input is;
      if (serializerStream != null) {
        if (needsFlush) {
          serializerStream.flush();
          needsFlush = false;
        }
      }
      try {
        is = storageFile != null && storageFile.exists()
            ? serializerInstance.newInput(new FileInputStream(storageFile))
            : null;
      } catch (FileNotFoundException e) {
        throw new IllegalStateException(
            "Failed to open spilling storage: " + storageFile, e);
      }

      Iterator elemsIter = elems.iterator();
      return new Iterator<T>() {
        @Override
        public boolean hasNext() {
          boolean n = (is != null && !is.eof()) || elemsIter.hasNext();
          if (!n && is != null) {
            is.close();
          }
          return n;
        }

        @SuppressWarnings("unchecked")
        @Override
        public T next() {
          if (is != null && !is.eof()) {
            return (T) is.readObject();
          }
          if (elemsIter.hasNext()) {
            return (T) elemsIter.next();
          }
          if (is != null) {
            is.close();
          }
          throw new NoSuchElementException();
        }
      };
    };
  }

  void closeOutput() {
    flush();
    if (serializerStream != null) {
      serializerStream.close();
      serializerStream = null;
    }
  }

  @Override
  public void clear() {
    if (serializerStream != null) {
      serializerStream.close();
      serializerStream = null;
    }
    if (storageFile != null && storageFile.exists()) {
      if (!storageFile.delete()) {
        throw new IllegalStateException(
            "Failed to clean up storage file: " + storageFile);
      }
    }
  }

  /**
   * Force persisting all the data to disk.
   */
  void flush() {
    spillElems();
    serializerStream.flush();
    needsFlush = false;
  }

  private void initDiskStorage() {
    assert storageFile == null;
    assert serializerInstance == null;
    assert serializerStream == null;
    storageFile = spillFileFactory.newSpillFile();
    serializerInstance = serializerFactory.newSerializer();
    try {
      serializerStream = serializerInstance.newOutput(new FileOutputStream(storageFile));
    } catch (FileNotFoundException e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  public Iterator<T> iterator() {
    return get().iterator();
  }

  @Override
  public void close() {
    clear();
  }


}
