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

import io.trinitylake.exception.StreamOpenFailureException;
import io.trinitylake.storage.SeekableInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;

public class LocalInputStream extends SeekableInputStream {

  private final File file;
  private FileInputStream stream;

  public LocalInputStream(File file) {
    try {
      this.file = file;
      open();
    } catch (IOException e) {
      throw new StreamOpenFailureException(e, "Fail to open file: %s", file);
    }
  }

  public FileChannel channel() {
    return stream.getChannel();
  }

  private void open() throws IOException {
    this.stream = new FileInputStream(file);
  }

  @Override
  public int read() throws IOException {
    return stream.read();
  }

  @Override
  public int available() throws IOException {
    return stream.available();
  }

  @Override
  public synchronized void mark(int readlimit) {
    stream.mark(readlimit);
  }

  @Override
  public boolean markSupported() {
    return stream.markSupported();
  }

  @Override
  public int read(byte[] b) throws IOException {
    return stream.read(b);
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    return stream.read(b, off, len);
  }

  @Override
  public synchronized void reset() throws IOException {
    stream.reset();
  }

  @Override
  public long skip(long n) throws IOException {
    return stream.skip(n);
  }

  @Override
  public long getPos() throws IOException {
    return 0;
  }

  @Override
  public void seek(long newPos) throws IOException {
    open();
    skip(newPos);
  }

  @Override
  public void close() throws IOException {
    stream.close();
  }
}
