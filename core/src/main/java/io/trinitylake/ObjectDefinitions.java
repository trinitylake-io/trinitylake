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

import io.trinitylake.exception.StorageReadFailureException;
import io.trinitylake.exception.StorageWriteFailureException;
import io.trinitylake.models.LakehouseDef;
import io.trinitylake.models.NamespaceDef;
import io.trinitylake.models.TableDef;
import io.trinitylake.storage.LakehouseStorage;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class ObjectDefinitions {

  private ObjectDefinitions() {}

  public static void writeLakehouseDef(
      LakehouseStorage storage, String path, LakehouseDef lakehouseDef) {

    try (OutputStream stream = storage.startCommit(path)) {
      lakehouseDef.writeTo(stream);
    } catch (IOException e) {
      throw new StorageWriteFailureException(
          e, "Failed to write lakehouse definition to storage path %s at %s", path, storage.root());
    }
  }

  public static LakehouseDef readLakehouseDef(LakehouseStorage storage, String path) {
    try (InputStream stream = storage.startRead(path)) {
      return LakehouseDef.parseFrom(stream);
    } catch (IOException e) {
      throw new StorageReadFailureException(
          e,
          "Failed to read lakehouse definition from storage path %s at %s",
          path,
          storage.root());
    }
  }

  public static void writeNamespaceDef(
      LakehouseStorage storage, String path, String namespaceName, NamespaceDef namespaceDef) {
    try (OutputStream stream = storage.startCommit(path)) {
      namespaceDef.writeTo(stream);
    } catch (IOException e) {
      throw new StorageWriteFailureException(
          e,
          "Failed to write namespace %s definition to storage path %s at %s",
          namespaceName,
          path,
          storage.root());
    }
  }

  public static NamespaceDef readNamespaceDef(LakehouseStorage storage, String path) {
    try (InputStream stream = storage.startRead(path)) {
      return NamespaceDef.parseFrom(stream);
    } catch (IOException e) {
      throw new StorageReadFailureException(
          e,
          "Failed to read namespace definition from storage path %s at %s",
          path,
          storage.root());
    }
  }

  public static void writeTableDef(
      LakehouseStorage storage,
      String path,
      String namespaceName,
      String tableName,
      TableDef tableDef) {
    try (OutputStream stream = storage.startCommit(path)) {
      tableDef.writeTo(stream);
    } catch (IOException e) {
      throw new StorageWriteFailureException(
          e,
          "Failed to write namespace %s table %s definition to storage path %s at %s",
          namespaceName,
          tableName,
          path,
          storage.root());
    }
  }

  public static TableDef readTableDef(LakehouseStorage storage, String path) {
    try (InputStream stream = storage.startRead(path)) {
      return TableDef.parseFrom(stream);
    } catch (IOException e) {
      throw new StorageReadFailureException(
          e, "Failed to read table definition from storage path %s at %s", path, storage.root());
    }
  }
}
