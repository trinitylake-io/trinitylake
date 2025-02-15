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

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.trinitylake.exception.StorageDeleteFailureException;
import io.trinitylake.exception.StorageReadFailureException;
import io.trinitylake.relocated.com.google.common.collect.Lists;
import io.trinitylake.relocated.com.google.common.collect.Maps;
import io.trinitylake.relocated.com.google.common.collect.Multimaps;
import io.trinitylake.relocated.com.google.common.collect.SetMultimap;
import io.trinitylake.relocated.com.google.common.collect.Sets;
import io.trinitylake.relocated.com.google.common.util.concurrent.MoreExecutors;
import io.trinitylake.relocated.com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.trinitylake.storage.AtomicOutputStream;
import io.trinitylake.storage.CommonStorageOpsProperties;
import io.trinitylake.storage.LiteralURI;
import io.trinitylake.storage.SeekableInputStream;
import io.trinitylake.storage.StorageOps;
import io.trinitylake.storage.local.LocalInputStream;
import io.trinitylake.util.FileUtil;
import io.trinitylake.util.Pair;
import java.io.File;
import java.io.OutputStream;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3AsyncClientBuilder;
import software.amazon.awssdk.services.s3.model.Delete;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectsResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.transfer.s3.S3TransferManager;
import software.amazon.awssdk.transfer.s3.model.DownloadFileRequest;
import software.amazon.awssdk.transfer.s3.model.FileDownload;

public class AmazonS3StorageOps implements StorageOps {

  private static final Logger LOG = LoggerFactory.getLogger(AmazonS3StorageOps.class);

  private static volatile ExecutorService executorService;

  private S3AsyncClient s3;
  private S3TransferManager transferManager;
  private CommonStorageOpsProperties commonProperties;
  private AmazonS3StorageOpsProperties s3Properties;

  private AtomicBoolean isResourceClosed = new AtomicBoolean(false);
  private Cache<LiteralURI, Pair<FileDownload, File>> preparedFiles;

  public AmazonS3StorageOps() {
    this(CommonStorageOpsProperties.instance(), AmazonS3StorageOpsProperties.instance());
  }

  public AmazonS3StorageOps(
      CommonStorageOpsProperties commonProperties, AmazonS3StorageOpsProperties s3Properties) {
    this.commonProperties = commonProperties;
    this.s3Properties = s3Properties;
    this.s3 = initializeS3AsyncClient(s3Properties);
    this.transferManager = S3TransferManager.builder().s3Client(s3).build();
    this.preparedFiles = initializePreparedFilesCache(commonProperties);
  }

  @Override
  public void initialize(Map<String, String> properties) {
    this.commonProperties = new CommonStorageOpsProperties(properties);
    this.s3Properties = new AmazonS3StorageOpsProperties(properties);
    this.s3 = initializeS3AsyncClient(s3Properties);
    this.transferManager = S3TransferManager.builder().s3Client(s3).build();
    this.preparedFiles = initializePreparedFilesCache(commonProperties);
  }

  private static S3AsyncClient initializeS3AsyncClient(AmazonS3StorageOpsProperties s3Properties) {
    S3AsyncClientBuilder builder = S3AsyncClient.builder();
    if (s3Properties.region() != null) {
      builder.region(Region.of(s3Properties.region()));
    }
    if (s3Properties.accessKeyId() != null) {
      if (s3Properties.sessionToken() != null) {
        builder.credentialsProvider(
            StaticCredentialsProvider.create(
                AwsSessionCredentials.create(
                    s3Properties.accessKeyId(),
                    s3Properties.secretAccessKey(),
                    s3Properties.sessionToken())));
      } else {
        builder.credentialsProvider(
            StaticCredentialsProvider.create(
                AwsBasicCredentials.create(
                    s3Properties.accessKeyId(), s3Properties.secretAccessKey())));
      }
    }
    return builder.build();
  }

  private static Cache<LiteralURI, Pair<FileDownload, File>> initializePreparedFilesCache(
      CommonStorageOpsProperties commonProperties) {
    return Caffeine.newBuilder()
        .expireAfterAccess(Duration.ofMillis(commonProperties.prepareReadCacheExpirationMillis()))
        .maximumSize(commonProperties.prepareReadCacheSize())
        .build();
  }

  @Override
  public CommonStorageOpsProperties commonProperties() {
    return commonProperties;
  }

  @Override
  public AmazonS3StorageOpsProperties systemSpecificProperties() {
    return s3Properties;
  }

  @Override
  public void prepareToReadLocal(LiteralURI uri) {
    try {
      File tempFile =
          FileUtil.createTempFile("s3-", commonProperties().prepareReadStagingDirectory());
      DownloadFileRequest downloadFileRequest =
          DownloadFileRequest.builder()
              .getObjectRequest(b -> b.bucket(uri.authority()).key(uri.path()))
              .destination(tempFile)
              .build();
      FileDownload downloadFile = transferManager.downloadFile(downloadFileRequest);
      preparedFiles.put(uri, Pair.of(downloadFile, tempFile));
    } catch (RuntimeException e) {
      LOG.warn("Failed to start preparing for file: {}", uri, e);
    }
  }

  @Override
  public LocalInputStream startReadLocal(LiteralURI uri) {
    Pair<FileDownload, File> fileDownloadResult = preparedFiles.getIfPresent(uri);
    if (fileDownloadResult != null) {
      prepareToReadLocal(uri);
      fileDownloadResult = preparedFiles.getIfPresent(uri);
    }

    try {
      fileDownloadResult.first().completionFuture().get();
      return new LocalInputStream(fileDownloadResult.second());
    } catch (ExecutionException | InterruptedException e) {
      throw new StorageReadFailureException(e);
    }
  }

  @Override
  public SeekableInputStream startRead(LiteralURI uri) {
    Pair<FileDownload, File> fileDownloadResult = preparedFiles.getIfPresent(uri);
    if (fileDownloadResult != null) {
      try {
        fileDownloadResult.first().completionFuture().get();
        return new LocalInputStream(fileDownloadResult.second());
      } catch (ExecutionException | InterruptedException e) {
        LOG.warn("Failed to prepare downloading file: {}", uri, e);
      }
    }
    LOG.info("Start read without preparation, directly open S3 object: {}", uri);
    return new S3InputStream(s3, uri);
  }

  @Override
  public boolean exists(LiteralURI uri) {
    try {
      s3.headObject(HeadObjectRequest.builder().bucket(uri.authority()).key(uri.path()).build())
          .get();
      return true;
    } catch (ExecutionException e) {
      return false;
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public AtomicOutputStream startCommit(LiteralURI uri) {
    return new S3AtomicOutputStream(s3, uri, commonProperties, s3Properties);
  }

  @Override
  public OutputStream startOverwrite(LiteralURI uri) {
    return new S3OverwriteOutputStream(s3, uri, commonProperties, s3Properties);
  }

  @Override
  public void delete(List<LiteralURI> uris) {
    SetMultimap<String, String> bucketToObjects =
        Multimaps.newSetMultimap(Maps.newHashMap(), Sets::newHashSet);
    List<Future<List<String>>> deletionTasks = Lists.newArrayList();

    for (LiteralURI uri : uris) {
      String bucket = uri.authority();
      String objectKey = uri.path();
      bucketToObjects.get(bucket).add(objectKey);
      if (bucketToObjects.get(bucket).size() == commonProperties().deleteBatchSize()) {
        Set<String> keys = Sets.newHashSet(bucketToObjects.get(bucket));
        Future<List<String>> deletionTask =
            executorService().submit(() -> deleteBatch(bucket, keys));
        deletionTasks.add(deletionTask);
        bucketToObjects.removeAll(bucket);
      }
    }

    // Delete the remainder
    for (Map.Entry<String, Collection<String>> bucketToObjectsEntry :
        bucketToObjects.asMap().entrySet()) {
      String bucket = bucketToObjectsEntry.getKey();
      Collection<String> keys = bucketToObjectsEntry.getValue();
      Future<List<String>> deletionTask = executorService().submit(() -> deleteBatch(bucket, keys));
      deletionTasks.add(deletionTask);
    }

    int totalFailedDeletions = 0;
    for (Future<List<String>> deletionTask : deletionTasks) {
      try {
        List<String> failedDeletions = deletionTask.get();
        failedDeletions.forEach(path -> LOG.warn("Failed to delete object at path {}", path));
        totalFailedDeletions += failedDeletions.size();
      } catch (ExecutionException e) {
        LOG.warn("Caught unexpected exception during batch deletion: ", e.getCause());
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        deletionTasks.stream().filter(task -> !task.isDone()).forEach(task -> task.cancel(true));
        throw new RuntimeException("Interrupted when waiting for deletions to complete", e);
      }
    }

    if (totalFailedDeletions > 0) {
      throw new StorageDeleteFailureException(
          "Failed to delete all files, remaining: %d", totalFailedDeletions);
    }
  }

  private List<String> deleteBatch(String bucket, Collection<String> keysToDelete) {
    List<ObjectIdentifier> objectIds =
        keysToDelete.stream()
            .map(key -> ObjectIdentifier.builder().key(key).build())
            .collect(Collectors.toList());
    DeleteObjectsRequest request =
        DeleteObjectsRequest.builder()
            .bucket(bucket)
            .delete(Delete.builder().objects(objectIds).build())
            .build();
    List<String> failures = Lists.newArrayList();
    try {
      DeleteObjectsResponse response = s3.deleteObjects(request).get();
      if (response.hasErrors()) {
        failures.addAll(
            response.errors().stream()
                .map(error -> String.format("s3://%s/%s", request.bucket(), error.key()))
                .collect(Collectors.toList()));
      }
    } catch (Exception e) {
      LOG.warn("Encountered failure when deleting batch", e);
      failures.addAll(
          request.delete().objects().stream()
              .map(obj -> String.format("s3://%s/%s", request.bucket(), obj.key()))
              .collect(Collectors.toList()));
    }
    return failures;
  }

  @Override
  public List<LiteralURI> list(LiteralURI prefix) {
    ListObjectsV2Request request =
        ListObjectsV2Request.builder().bucket(prefix.authority()).prefix(prefix.path()).build();

    List<LiteralURI> result = Lists.newArrayList();
    s3.listObjectsV2Paginator(request)
        .flatMapIterable(ListObjectsV2Response::contents)
        .map(obj -> new LiteralURI(prefix.scheme(), prefix.authority(), obj.key()))
        .subscribe(result::add);
    return result;
  }

  private ExecutorService executorService() {
    if (executorService == null) {
      synchronized (AmazonS3StorageOps.class) {
        if (executorService == null) {
          executorService =
              MoreExecutors.getExitingExecutorService(
                  (ThreadPoolExecutor)
                      Executors.newFixedThreadPool(
                          Runtime.getRuntime().availableProcessors(),
                          new ThreadFactoryBuilder()
                              .setDaemon(true)
                              .setNameFormat("trinitylake-s3-%d")
                              .build()));
        }
      }
    }

    return executorService;
  }

  @Override
  public void close() {
    // handles concurrent calls to close()
    if (isResourceClosed.compareAndSet(false, true)) {
      if (s3 != null) {
        s3.close();
      }
    }
  }
}
