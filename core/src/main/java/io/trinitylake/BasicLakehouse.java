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
package io.trinitylake;

import io.trinitylake.exception.ObjectNotFoundException;
import io.trinitylake.exception.StorageWriteFailureException;
import io.trinitylake.storage.FilePaths;
import io.trinitylake.storage.Storage;
import io.trinitylake.tree.ArrowTreeRoot;
import io.trinitylake.tree.TreeRoot;
import io.trinitylake.util.FileUtil;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.util.*;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.Field;

public class BasicLakehouse implements Lakehouse {

  private final Storage storage;

  public BasicLakehouse(Storage storage) {
    this.storage = storage;
  }

  @Override
  public TransactionContext beginTransaction(Map<String, String> options) {
    return ImmutableTransactionContext.builder()
        .options(options)
        .startedAtMillis(System.currentTimeMillis())
        .transactionId(UUID.randomUUID().toString())
        .version(loadLatestVersion())
        .build();
  }

  @Override
  public LakehouseVersion commitTransaction(TransactionContext version, TreeRoot root) {
    LakehouseVersion currentVersion = loadLatestVersion();
    String nextVersionFilePath = FilePaths.rootNodeFilePath(currentVersion.version() + 1);
    OutputStream stream = storage.startWrite(nextVersionFilePath);
    writeNewVersion(stream, root);
    return null;
  }

  @Override
  public LakehouseVersion loadLatestVersion() {
    String latestVersionHintText =
        FileUtil.readToString(storage.startRead(FilePaths.LATEST_VERSION_HINT));
    long latestVersion = Long.parseLong(latestVersionHintText);
    String versionFilePath = FilePaths.rootNodeFilePath(latestVersion);
    while (storage.exists(versionFilePath)) {
      latestVersion++;
      versionFilePath = FilePaths.rootNodeFilePath(latestVersion);
    }

    TreeRoot root = new ArrowTreeRoot(storage, versionFilePath);
    return new BasicLakehouseVersion(storage, root);
  }

  @Override
  public LakehouseVersion loadVersion(long version) {
    LakehouseVersion latestVersion = loadLatestVersion();
    if (version > latestVersion.version()) {
      throw new ObjectNotFoundException(
          "Version %d is higher than latest version %d", version, latestVersion.version());
    }

    LakehouseVersion resultVersion = latestVersion;
    while (resultVersion.previousVersion() != null && resultVersion.version() > version) {
      resultVersion = resultVersion.previousVersion();
    }

    if (resultVersion.version() != version) {
      throw new ObjectNotFoundException(
          "Cannot find version %d, cloest version: %d", version, resultVersion.version());
    }

    return resultVersion;
  }

  @Override
  public Iterable<LakehouseVersion> listVersions() {
    return new LakehouseVersionIterable(loadLatestVersion());
  }

  private static class LakehouseVersionIterable implements Iterable<LakehouseVersion> {

    private final LakehouseVersion latestVersion;

    public LakehouseVersionIterable(LakehouseVersion latestVersion) {
      this.latestVersion = latestVersion;
    }

    @Override
    public Iterator<LakehouseVersion> iterator() {
      return new LakehouseVersionIterator(latestVersion);
    }
  }

  private static class LakehouseVersionIterator implements Iterator<LakehouseVersion> {

    private LakehouseVersion currentVersion;

    public LakehouseVersionIterator(LakehouseVersion currentVersion) {
      this.currentVersion = currentVersion;
    }

    @Override
    public boolean hasNext() {
      return currentVersion.previousVersion() != null;
    }

    @Override
    public LakehouseVersion next() {
      this.currentVersion = currentVersion.previousVersion();
      return currentVersion;
    }
  }

  private void writeNewVersion(OutputStream stream, TreeRoot root) {
    BufferAllocator allocator = new RootAllocator();
    BitVector bitVector = new BitVector("boolean", allocator);
    VarCharVector varCharVector = new VarCharVector("varchar", allocator);
    for (int i = 0; i < 10; i++) {
      bitVector.setSafe(i, i % 2 == 0 ? 0 : 1);
      varCharVector.setSafe(i, ("test" + i).getBytes(StandardCharsets.UTF_8));
    }
    bitVector.setValueCount(10);
    varCharVector.setValueCount(10);

    List<Field> fields = Arrays.asList(bitVector.getField(), varCharVector.getField());
    List<FieldVector> vectors = Arrays.asList(bitVector, varCharVector);
    VectorSchemaRoot schema = new VectorSchemaRoot(fields, vectors);
    try (ArrowStreamWriter writer =
        new ArrowStreamWriter(schema, null, Channels.newChannel(stream))) {
      writer.start();
      writer.writeBatch();

      for (int i = 0; i < 4; i++) {
        BitVector childVector1 = (BitVector) schema.getVector(0);
        VarCharVector childVector2 = (VarCharVector) schema.getVector(1);
        childVector1.reset();
        childVector2.reset();
        writer.writeBatch();
      }

      writer.end();
    } catch (IOException e) {
      throw new StorageWriteFailureException(e);
    }
  }
}
