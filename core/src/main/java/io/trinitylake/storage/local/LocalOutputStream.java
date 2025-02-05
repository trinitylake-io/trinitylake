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

import io.trinitylake.exception.CommitFailureException;
import io.trinitylake.exception.StorageAtomicSealFailureException;
import io.trinitylake.exception.StoragePathNotFoundException;
import io.trinitylake.storage.AtomicOutputStream;
import io.trinitylake.storage.CommonStorageOpsProperties;
import io.trinitylake.util.FileUtil;
import java.io.*;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LocalOutputStream extends AtomicOutputStream {

  private static final Logger LOG = LoggerFactory.getLogger(LocalOutputStream.class);

  private final Path file;
  private final File tempFile;
  private final FileOutputStream stream;

  public LocalOutputStream(
      Path file,
      CommonStorageOpsProperties commonProperties,
      LocalStorageOpsProperties localProperties) {
    this.file = file;
    this.tempFile = FileUtil.createTempFile("local-", commonProperties.writeStagingDirectory());
    try {
      this.stream = new FileOutputStream(tempFile);
    } catch (FileNotFoundException e) {
      throw new StoragePathNotFoundException(e);
    }
  }

  @Override
  public void atomicallySeal() throws CommitFailureException, IOException {
    try {
      // this would result in potential orphan directories,
      // but there is not a better way at this moment
      // plus with the file path optimization strategy,
      // it is okay to create these folders since they would be used eventually
      Files.createDirectories(file.getParent());
      Files.move(tempFile.toPath(), file);
    } catch (FileAlreadyExistsException e) {
      throw new StorageAtomicSealFailureException(e);
    }
  }

  @Override
  public void write(byte[] bytes) throws IOException {
    stream.write(bytes);
  }

  @Override
  public void write(int b) throws IOException {
    stream.write(b);
  }

  @Override
  public void write(byte[] bytes, int off, int len) throws IOException {
    stream.write(bytes, off, len);
  }

  @Override
  public void flush() throws IOException {
    stream.flush();
  }
}
