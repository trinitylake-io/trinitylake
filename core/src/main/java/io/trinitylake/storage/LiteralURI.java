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

import io.trinitylake.util.ValidationUtil;
import java.util.Objects;

/**
 * A URI that should be interpreted literally. Special characters like `..` and `.` should be
 * prohibited.
 */
public class LiteralURI {
  private static final String SCHEME_DELIM = "://";
  private static final String PATH_DELIM = "/";
  private static final String QUERY_DELIM = "\\?";
  private static final String FRAGMENT_DELIM = "#";

  private final String location;
  private final String scheme;
  private final String authority;
  private final String path;

  public LiteralURI(String scheme, String authority, String path) {
    ValidationUtil.checkNotNull(scheme, "Scheme cannot be null.");
    ValidationUtil.checkNotNull(authority, "Authority cannot be null.");
    ValidationUtil.checkNotNull(path, "Path cannot be null.");

    this.scheme = scheme;
    this.authority = authority;
    this.path = path;
    this.location = scheme + SCHEME_DELIM + authority + PATH_DELIM + path;
  }

  public LiteralURI(String location) {
    ValidationUtil.checkNotNull(location, "Location cannot be null.");

    // TODO: checks
    this.location = location;
    String[] schemeSplit = location.split(SCHEME_DELIM, -1);
    ValidationUtil.checkArgument(
        schemeSplit.length == 2, "Invalid URI, cannot determine scheme: %s", location);
    this.scheme = schemeSplit[0];

    String[] authoritySplit = schemeSplit[1].split(PATH_DELIM, 2);

    this.authority = authoritySplit[0];

    // Strip query and fragment if they exist
    String path = authoritySplit.length > 1 ? authoritySplit[1] : "";
    path = path.split(QUERY_DELIM, -1)[0];
    path = path.split(FRAGMENT_DELIM, -1)[0];
    this.path = path;
  }

  public LiteralURI extendPath(String newPath) {
    ValidationUtil.checkNotNull(newPath, "newPath must not be null");
    return new LiteralURI(scheme, authority, path + PATH_DELIM + newPath);
  }

  public String authority() {
    return authority;
  }

  public String path() {
    return path;
  }

  public String scheme() {
    return scheme;
  }

  @Override
  public String toString() {
    return location;
  }

  public LiteralURI toDirectory() {
    if (path.endsWith(PATH_DELIM)) {
      return this;
    }
    return new LiteralURI(String.format("%s://%s/%s/", scheme, authority, path));
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    LiteralURI that = (LiteralURI) o;
    return Objects.equals(location, that.location)
        && Objects.equals(scheme, that.scheme)
        && Objects.equals(authority, that.authority)
        && Objects.equals(path, that.path);
  }

  @Override
  public int hashCode() {
    return Objects.hash(location, scheme, authority, path);
  }
}
