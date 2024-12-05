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

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.io.ByteStreams;
import dev.failsafe.Failsafe;
import dev.failsafe.FailsafeException;
import dev.failsafe.RetryPolicy;
import io.trinitylake.exception.StoragePathNotFoundException;
import io.trinitylake.exception.StorageReadFailureException;
import io.trinitylake.storage.SeekableInputStream;
import io.trinitylake.storage.URI;
import java.io.IOException;
import java.io.InputStream;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import javax.net.ssl.SSLException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.http.Abortable;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;

class S3InputStream extends SeekableInputStream {
  private static final Logger LOG = LoggerFactory.getLogger(S3InputStream.class);

  private static final List<Class<? extends Throwable>> RETRYABLE_EXCEPTIONS =
      ImmutableList.of(SSLException.class, SocketTimeoutException.class, SocketException.class);

  private final StackTraceElement[] createStack;
  private final S3AsyncClient s3;
  private final URI uri;

  private InputStream stream;
  private long pos = 0;
  private long next = 0;
  private boolean closed = false;

  private int skipSize = 1024 * 1024;
  private RetryPolicy<Object> retryPolicy =
      RetryPolicy.builder()
          .handle(RETRYABLE_EXCEPTIONS)
          .onRetry(
              e -> {
                LOG.warn(
                    "Retrying read from S3, reopening stream (attempt {})", e.getAttemptCount());
                resetForRetry();
              })
          .onFailure(
              e ->
                  LOG.error(
                      "Failed to read from S3 input stream after exhausting all retries",
                      e.getException()))
          .withMaxRetries(3)
          .build();

  public S3InputStream(S3AsyncClient s3, URI uri) {
    this.s3 = s3;
    this.uri = uri;

    this.createStack = Thread.currentThread().getStackTrace();
  }

  @Override
  public long getPos() {
    return next;
  }

  @Override
  public void seek(long newPos) {
    Preconditions.checkState(!closed, "already closed");
    Preconditions.checkArgument(newPos >= 0, "position is negative: %s", newPos);

    // this allows a seek beyond the end of the stream but the next read will fail
    next = newPos;
  }

  @Override
  public int read() throws IOException {
    Preconditions.checkState(!closed, "Cannot read: already closed");
    positionStream();
    try {
      int bytesRead = Failsafe.with(retryPolicy).get(() -> stream.read());
      pos += 1;
      next += 1;

      return bytesRead;
    } catch (FailsafeException ex) {
      if (ex.getCause() instanceof IOException) {
        throw (IOException) ex.getCause();
      }

      throw ex;
    }
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    Preconditions.checkState(!closed, "Cannot read: already closed");
    positionStream();

    try {
      int bytesRead = Failsafe.with(retryPolicy).get(() -> stream.read(b, off, len));
      pos += bytesRead;
      next += bytesRead;

      return bytesRead;
    } catch (FailsafeException ex) {
      if (ex.getCause() instanceof IOException) {
        throw (IOException) ex.getCause();
      }

      throw ex;
    }
  }

  @Override
  public void close() throws IOException {
    super.close();
    closed = true;
    closeStream(false);
  }

  private void positionStream() throws IOException {
    if ((stream != null) && (next == pos)) {
      // already at specified position
      return;
    }

    if ((stream != null) && (next > pos)) {
      // seeking forwards
      long skip = next - pos;
      if (skip <= Math.max(stream.available(), skipSize)) {
        // already buffered or seek is small enough
        LOG.debug("Read-through seek for {} to offset {}", uri, next);
        try {
          ByteStreams.skipFully(stream, skip);
          pos = next;
          return;
        } catch (IOException ignored) {
          // will retry by re-opening the stream
        }
      }
    }

    // close the stream and open at desired position
    LOG.debug("Seek with new stream for {} to offset {}", uri, next);
    pos = next;
    openStream();
  }

  private void openStream() throws IOException {
    openStream(false);
  }

  private void openStream(boolean closeQuietly) throws IOException {
    GetObjectRequest.Builder requestBuilder =
        GetObjectRequest.builder()
            .bucket(uri.authority())
            .key(uri.path())
            .range(String.format("bytes=%s-", pos));
    closeStream(closeQuietly);

    try {
      stream =
          s3.getObject(requestBuilder.build(), AsyncResponseTransformer.toBlockingInputStream())
              .get();
    } catch (NoSuchKeyException e) {
      throw new StoragePathNotFoundException(e, "Path does not exist: %s", uri);
    } catch (ExecutionException e) {
      throw new StorageReadFailureException(e, "Read execution failed: %s", uri);
    } catch (InterruptedException e) {
      throw new StorageReadFailureException(e, "Read interrupted: %s", uri);
    }
  }

  void resetForRetry() throws IOException {
    openStream(true);
  }

  private void closeStream(boolean closeQuietly) throws IOException {
    if (stream != null) {
      // if we aren't at the end of the stream, and the stream is abortable, then
      // call abort() so we don't read the remaining data with the Apache HTTP client
      abortStream();
      try {
        stream.close();
      } catch (IOException e) {
        if (closeQuietly) {
          stream = null;
          LOG.warn("An error occurred while closing the stream", e);
          return;
        }

        // the Apache HTTP client will throw a ConnectionClosedException
        // when closing an aborted stream, which is expected
        if (!e.getClass().getSimpleName().equals("ConnectionClosedException")) {
          throw e;
        }
      }
      stream = null;
    }
  }

  private void abortStream() {
    try {
      if (stream instanceof Abortable && stream.read() != -1) {
        ((Abortable) stream).abort();
      }
    } catch (Exception e) {
      LOG.warn("An error occurred while aborting the stream", e);
    }
  }

  public void setSkipSize(int skipSize) {
    this.skipSize = skipSize;
  }

  @SuppressWarnings({"checkstyle:NoFinalizer", "Finalize"})
  @Override
  protected void finalize() throws Throwable {
    super.finalize();
    if (!closed) {
      close(); // releasing resources is more important than printing the warning
      String trace = Joiner.on("\n\t").join(Arrays.copyOfRange(createStack, 1, createStack.length));
      LOG.warn("Unclosed input stream created by:\n\t{}", trace);
    }
  }
}
