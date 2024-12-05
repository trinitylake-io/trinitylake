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
package io.trinitylake.util;

import io.trinitylake.exception.StreamOpenFailureException;
import java.io.File;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileUtil {

  private static final Logger LOG = LoggerFactory.getLogger(FileUtil.class);

  public static File createTempFile(String filePrefix, File stagingDirectory) {
    try {
      return File.createTempFile("trinitylake-" + filePrefix, ".tmp", stagingDirectory);
    } catch (IOException e) {
      throw new StreamOpenFailureException(
          e, "Fail to open new temp file at: %s", stagingDirectory);
    }
  }

  public static void createStagingDirectoryIfNotExists(File stagingDirectory) {
    if (!stagingDirectory.exists()) {
      LOG.info(
          "Staging directory does not exist, trying to create one: {}",
          stagingDirectory.getAbsolutePath());
      boolean createdStagingDirectory = stagingDirectory.mkdirs();
      if (createdStagingDirectory) {
        LOG.info("Successfully created staging directory: {}", stagingDirectory.getAbsolutePath());
      } else {
        if (stagingDirectory.exists()) {
          LOG.info(
              "Successfully created staging directory by another process: {}",
              stagingDirectory.getAbsolutePath());
        } else {
          throw new StreamOpenFailureException(
              "Failed to create staging directory due to some unknown reason: "
                  + stagingDirectory.getAbsolutePath());
        }
      }
    }
  }
}
