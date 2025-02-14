/*
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
package io.trinitylake.storage.local;

import io.trinitylake.exception.StorageAtomicSealFailureException;
import io.trinitylake.storage.AtomicOutputStream;
import io.trinitylake.storage.CommonStorageOpsProperties;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;

public class LocalAtomicStagingOutputStream extends AtomicOutputStream {
  private final LocalStagingOutputStream delegate;

  public LocalAtomicStagingOutputStream(
      Path file,
      CommonStorageOpsProperties commonProperties,
      LocalStorageOpsProperties localProperties) {
    this.delegate =
        new LocalStagingOutputStream(file, commonProperties, localProperties) {
          @Override
          protected void commit() throws IOException {
            // this would result in potential orphan directories,
            // but there is not a better way at this moment
            // plus with the file path optimization strategy,
            // it is okay to create these folders since they would be used eventually
            createParentDirectories();
            Files.move(tempFile.toPath(), file);
          }
        };
  }

  @Override
  public void atomicallySeal() throws StorageAtomicSealFailureException, IOException {
    try {
      delegate.commit();
    } catch (FileAlreadyExistsException e) {
      throw new StorageAtomicSealFailureException(e);
    }
  }

  @Override
  public FileChannel channel() {
    return delegate.getChannel();
  }

  @Override
  public void write(int b) throws IOException {
    delegate.write(b);
  }

  @Override
  public void write(byte[] bytes) throws IOException {
    delegate.write(bytes);
  }

  @Override
  public void close() throws IOException {
    atomicallySeal();
  }
}
