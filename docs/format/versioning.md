# Versioning

The TrinityLake format is released independently.
This means the format version could differ from the format implementation version.
For example, you can have TrinityLake format `1.2.0`, Java SDK `1.5.1`.

## Compatibility Definitions

We in general expect there to be 2 actors, **Readers** and **Writers**.
Typically, reader and writer versions are at the same version,
but a system can also choose to implement a reader and writer that are of different versions.

### Backward Compatible

A format version is backward compatible if the format that is produced by a lower version writer
can be correctly read by a higher version reader.

### Forward Compatible

A format version is forward compatible if the format that is produced by a higher version writer
can be correctly read by a lower version reader.

## Versioning Semantics

The TrinityLake format uses traditional major, minor and patch versioning semantics, forming a version string like `1.2.3`.
We expect to bump up:

- Major version when the format introduces forward incompatible changes.
- Minor version for any new feature release in the format that is still forward compatible.
- Patch version if there are bugs, typos, improvements in wording, additional explanations, etc. that are added to the format.

## Format Implementation Expectations

In general, a TrinityLake format implementation is expected to be always backward compatible for all past versions,
until a past version is declared as deprecated.

Because of the backward and forward compatibility requirement, minor and patch versions are for information only
so that people can know what has been changing and update their implementations accordingly.
This is also why only the major version is directly recorded in the [Lakehouse definition](definitions/lakehouse.md).

It is recommended that format implementations explicitly check the format major version and 
fail the reader or writer accordingly for unsupported major version.