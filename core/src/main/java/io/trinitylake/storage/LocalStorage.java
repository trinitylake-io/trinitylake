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
package io.trinitylake.storage;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;

public class LocalStorage implements Storage {

  private final String root;

  public LocalStorage(String root) {
    this.root = root;
  }

  @Override
  public InputStream get(String key) throws IOException {
    return Files.newInputStream(new File(root + "/" + key).toPath());
  }

  @Override
  public void putIfNotExist(String key, InputStream value) throws IOException {
    OutputStream outStream = Files.newOutputStream(new File(root + "/" + key).toPath());

    byte[] buffer = new byte[8 * 1024];
    int bytesRead;
    while ((bytesRead = value.read(buffer)) != -1) {
      outStream.write(buffer, 0, bytesRead);
    }
    outStream.close();
  }

  @Override
  public String root() {
    return root;
  }
}
