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
import io.trinitylake.exception.StoragePathNotFoundException;
import io.trinitylake.storage.AtomicOutputStream;
import io.trinitylake.storage.CommonStorageOpsProperties;
import io.trinitylake.util.FileUtil;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;

public class LocalOutputStream extends AtomicOutputStream {

  private final File file;
  private final File tempFile;
  private final FileOutputStream stream;

  public LocalOutputStream(
      File file,
      CommonStorageOpsProperties commonProperties,
      LocalStorageOpsProperties localProperties) {
    this.file = file;
    this.tempFile =
        FileUtil.createTempFile("lcoal-output", commonProperties.writeStagingDirectory());
    try {
      this.stream = new FileOutputStream(tempFile);
    } catch (FileNotFoundException e) {
      throw new StoragePathNotFoundException(e);
    }
  }

  @Override
  public void seal() throws CommitFailureException, IOException {
    Files.move(tempFile.toPath(), file.toPath(), StandardCopyOption.ATOMIC_MOVE);
  }

  @Override
  public void write(byte[] b) throws IOException {
    stream.write(b);
  }

  @Override
  public void write(int b) throws IOException {
    stream.write(b);
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    stream.write(b, off, len);
  }

  @Override
  public void flush() throws IOException {
    stream.flush();
  }
}
