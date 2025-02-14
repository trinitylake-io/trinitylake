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

import io.trinitylake.exception.StoragePathNotFoundException;
import io.trinitylake.storage.CommonStorageOpsProperties;
import io.trinitylake.util.FileUtil;
import java.io.*;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class LocalStagingOutputStream extends OutputStream {
  private static final Logger LOG = LoggerFactory.getLogger(LocalStagingOutputStream.class);

  protected final Path file;
  protected final File tempFile;
  protected final FileOutputStream stream;

  protected LocalStagingOutputStream(
      Path file,
      CommonStorageOpsProperties commonProperties,
      LocalStorageOpsProperties localProperties) {
    this.file = file;
    this.tempFile = FileUtil.createTempFile("local-", commonProperties.writeStagingDirectory());
    LOG.debug("Created temporary file for staging write: {}", tempFile);
    try {
      this.stream = new FileOutputStream(tempFile);
    } catch (FileNotFoundException e) {
      throw new StoragePathNotFoundException(e);
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

  protected FileChannel getChannel() {
    return stream.getChannel();
  }

  protected void createParentDirectories() throws IOException {
    Files.createDirectories(file.getParent());
  }

  protected abstract void commit() throws IOException;

  @Override
  public void close() throws IOException {
    try {
      commit();
    } finally {
      stream.close();
    }
  }
}
