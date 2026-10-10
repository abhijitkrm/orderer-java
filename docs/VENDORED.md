# VENDORED — upstream sources

orderer-java vendors two things, both **verbatim**. Never edit them here.

## 1. The orderer spec (verified)

`spec/` and `vectors/` are copied from the
[orderer](https://github.com/abhijitkrm/orderer) spec repo. They include
matcher's spec and corpus.

- **upstream**: `orderer`
- **repo**: `https://github.com/abhijitkrm/orderer`
- **commit**: `f54438147277b8834eba0b3facde71b5356f81c7`
- **tag**: `orderer-spec/1.3`
- **paths**: `spec=spec vectors=vectors`

`docs/VENDORED.sha256` holds every file's checksum. `scripts/vendored.sh`
verifies the copy against it and, when `../orderer` is checked out,
against the pinned commit.

## 2. The matching core

`src/main/java/io/github/abhijitkrm/matcher/` is [matcher-java](https://github.com/abhijitkrm/matcher-java)'s
package of the same name at `20e563bc0186c6d52b2dbce64d4efce3fd244f3f`, byte for byte, which includes the ladder rescan fix (20e563b). matcher-java
never had the OrderMap deletion bug fixed upstream in matcher-rust and
matcher-cpp; `vectors/regress/001_dense_map_churn` pins that.

To check it:

```bash
git -C ../matcher-java diff --stat 20e563bc0186c6d52b2dbce64d4efce3fd244f3f -- src/main/java/io/github/abhijitkrm/matcher \
  && diff -r ../matcher-java/src/main/java/io/github/abhijitkrm/matcher src/main/java/io/github/abhijitkrm/matcher
```

orderer's strict parsing (`Flat.java`) wraps the core rather than changing
it. matcher-java's own `JsonFlat` is lenient: malformed fields become 0 or
null. The orderer harnesses must reject them (spec/HARNESS.md §5).
