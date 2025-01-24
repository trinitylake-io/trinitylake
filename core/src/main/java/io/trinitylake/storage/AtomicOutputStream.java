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

import io.trinitylake.exception.CommitFailureException;
import java.io.IOException;
import java.io.OutputStream;

public abstract class AtomicOutputStream extends OutputStream {

  /**
   * Atomically seal the file that is being written to
   *
   * @throws CommitFailureException if the sealing process fails due to atomicity conflict
   * @throws IOException for any other failure in write
   */
  public abstract void seal() throws CommitFailureException, IOException;

  @Override
  public void close() throws IOException {
    super.close();
  }
}
