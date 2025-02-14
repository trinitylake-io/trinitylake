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
package io.trinitylake.storage.s3;

import io.trinitylake.exception.StorageWriteFailureException;
import io.trinitylake.storage.CommonStorageOpsProperties;
import io.trinitylake.storage.LiteralURI;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

class S3OverwriteOutputStream extends S3StagingOutputStream {
  public S3OverwriteOutputStream(
      S3AsyncClient s3,
      LiteralURI uri,
      CommonStorageOpsProperties commonProperties,
      AmazonS3StorageOpsProperties s3Properties) {
    super(s3, uri, commonProperties, s3Properties);
  }

  @Override
  protected void commit() throws IOException {
    if (closed) {
      return;
    }

    closed = true;
    try {
      stagingFileStream.close();
      s3.putObject(
              PutObjectRequest.builder().bucket(uri.authority()).key(uri.path()).build(),
              AsyncRequestBody.fromFile(stagingFile))
          .get();
    } catch (ExecutionException | InterruptedException e) {
      throw new StorageWriteFailureException(
          e, "Fail to upload to %s from staging file: %s", uri, stagingFile);
    }
  }
}
