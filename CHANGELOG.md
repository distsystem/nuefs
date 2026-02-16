# Changelog

## [0.2.0](https://github.com/distsystem/nuefs/compare/nuefs-v0.1.0...nuefs-v0.2.0) (2026-02-16)


### ⚠ BREAKING CHANGES

* replace ManifestBuilder with Lock compile/save
* drop Mapping and accept ManifestEntry lists

### Features

* add mount resolution by root ([7e695b3](https://github.com/distsystem/nuefs/commit/7e695b3f076ca6566baebb9ec363eaebb58bdabf))
* add nuefsd daemon and JSON protocol ([01d884d](https://github.com/distsystem/nuefs/commit/01d884d908e0fd7c977a677c8e5da6b006bfe5a3))
* add uposixtest — unprivileged POSIX filesystem conformance tool ([385c774](https://github.com/distsystem/nuefs/commit/385c774957604f9d3bc181a26d7c0f4dbb76740e))
* **cli:** add mount dry-run and persist lockfile ([5c301d1](https://github.com/distsystem/nuefs/commit/5c301d1f21f9dcb36abb88e5ba2093c185f3612d))
* **cli:** add nue start and default socket path ([79d2912](https://github.com/distsystem/nuefs/commit/79d2912c4cfb4ed0a95cdae241ef31075a9312cb))
* **cli:** add stop command and socket cleanup ([897a5bc](https://github.com/distsystem/nuefs/commit/897a5bc22bce7fa57348f0396ccb91cb5334335f))
* **cli:** add workspace lock/apply commands ([9094bff](https://github.com/distsystem/nuefs/commit/9094bff7205d7e5f07e34bb032385662a27021db))
* **client:** auto-start nuefsd when missing ([33b266b](https://github.com/distsystem/nuefs/commit/33b266b9401c9482a3a1eb5f0b8936faf58f232a))
* **daemon:** add RPC shutdown and stop command ([a6c6c7f](https://github.com/distsystem/nuefs/commit/a6c6c7f8fbbf7887d8e53bed37cbed54dd26aa12))
* **daemon:** add tracing logs and test workspace fixtures ([c4f3211](https://github.com/distsystem/nuefs/commit/c4f3211f3d8040246d4a9d886bfe55267f3f3d1f))
* **daemon:** support file mounts and better FUSE notifications ([f9ed416](https://github.com/distsystem/nuefs/commit/f9ed416921fe0b82b151407688d3c1e5f75b3641))
* **fuse:** add readlink, symlink, and hardlink support ([a010945](https://github.com/distsystem/nuefs/commit/a0109458f0301e8ad0eafe6629de5e3cdc048ccf))
* generate Python stubs for extension ([912c0da](https://github.com/distsystem/nuefs/commit/912c0dad0e828ec2ce57a9355645604ec4db6988))
* **manifest:** add Python nue.yaml models and init script ([e4fb85b](https://github.com/distsystem/nuefs/commit/e4fb85b25deb881cb5f371f404f4ae4e6171a3b1))
* **manifest:** optionally expand mounts via git index ([3cdccbb](https://github.com/distsystem/nuefs/commit/3cdccbbfa69da3dda89c2af9c64ebb0136c8c353))
* **mount:** pass mount roots for filesystem scanning ([8184895](https://github.com/distsystem/nuefs/commit/81848956548b2452ddf4ff99ae8baa9eb9e686e9))
* **mount:** support include/exclude patterns ([f9adcbc](https://github.com/distsystem/nuefs/commit/f9adcbc807142c4951e41ef675e8676749c7a42d))
* **python:** add Pydantic manifest models ([8f75fad](https://github.com/distsystem/nuefs/commit/8f75fad4271e06780f7bcb9944f36f69dc280e24))
* **python:** externalize gitdir on mount ([76a9cd3](https://github.com/distsystem/nuefs/commit/76a9cd3063ccb111638c45d298594f5c58044da8))
* **status:** expose daemon info and show uptime ([0476813](https://github.com/distsystem/nuefs/commit/047681343a407c56f994e4b08200348efbb073c0))
* support updating mount manifest at runtime ([a7e33da](https://github.com/distsystem/nuefs/commit/a7e33dab6d8508092b012e2b20b5b1704e077ca7))
* **test:** add fio performance benchmark ([74e62c6](https://github.com/distsystem/nuefs/commit/74e62c6bc3e27412c62d2cbd82f6a111290a93a2))
* **workspace:** add nue.yaml workflow with lock/apply ([967589d](https://github.com/distsystem/nuefs/commit/967589d7093e9fecf245c1a5a129d26ae681d371))


### Bug Fixes

* **ci:** correct conda package glob path (no linux-64 subdir) ([617e49e](https://github.com/distsystem/nuefs/commit/617e49ec5be4f30a9361da022ce9a1d9d7dc2204))
* **ci:** install both nuefs and nuefsd in test-mundi-install ([2cd6a4b](https://github.com/distsystem/nuefs/commit/2cd6a4b5079eafe1e863baccbc9fc23aa1e49f86))
* **ci:** specify pixi build output directory ([1517ad3](https://github.com/distsystem/nuefs/commit/1517ad3f1445ccecf64cf8922b359075be9af68b))
* **ci:** use folded scalar for setup-pixi environments ([2cb1a47](https://github.com/distsystem/nuefs/commit/2cb1a47f0f1c52c0bb37b07b2894b221967a0a53))
* **cli:** handle unmount and daemon spawn failures ([b81651d](https://github.com/distsystem/nuefs/commit/b81651da0f8d95d72e0f7338d9f511e57fab484c))
* **cli:** improve unmount when daemon not running ([5f71ee7](https://github.com/distsystem/nuefs/commit/5f71ee74fdc6d60e232670139dea09acab4e164b))
* **daemon:** avoid importing .git into union ([42d1a14](https://github.com/distsystem/nuefs/commit/42d1a14720ea8751513a0e3c4ec5978cdd79e664))
* **daemon:** refresh entries on manifest update ([504ad5f](https://github.com/distsystem/nuefs/commit/504ad5f4d7ef1fe918646967af851c2aab661032))
* **fuse:** bypass self-recursion for real paths ([7f1b48c](https://github.com/distsystem/nuefs/commit/7f1b48c708285c81ff6ef360c926d4d015c80d7a))
* **fuse:** implement statfs and correct rename parent resolution ([90387dc](https://github.com/distsystem/nuefs/commit/90387dcee91f0cd77303bacb7bbb1daf87308444))
* **fuse:** open real paths via openat flags ([6d83f8f](https://github.com/distsystem/nuefs/commit/6d83f8f981aedfb75482a66330fd2039dec25976))
* **fuse:** preserve original errno instead of mapping all errors to EIO ([1a58856](https://github.com/distsystem/nuefs/commit/1a58856c018d8d95c7e9084b80880f4822e7999c))
* **fuse:** remove readdirplus override that silently dropped entries ([609f472](https://github.com/distsystem/nuefs/commit/609f472763c66b465346e4eafe268230b3da75bd))
* **fuse:** stable generation number and panic hook ([2aa3030](https://github.com/distsystem/nuefs/commit/2aa30305b12dfcf80e888f085243726a06ca8db8))
* **fuse:** use fstat(fd) in getattr when file handle is available ([2deb6b9](https://github.com/distsystem/nuefs/commit/2deb6b91762c2846cb96e8728ef40e59cf6e8350))
* **manifest:** add root mount and drop incorrect ownership fallback ([d688089](https://github.com/distsystem/nuefs/commit/d688089f0013369a1b16017065f6f20eb28d1f9e))
* **manifest:** skip vanished mount entries during scan ([200fe6a](https://github.com/distsystem/nuefs/commit/200fe6a1b74f2ce4107639d53e17d4de32cb7ff9))
* **mount:** cascade path resolution for delete operations ([16466be](https://github.com/distsystem/nuefs/commit/16466bef94e9404bc3b574b911eac1f787a1b6f8))
* **test:** align TestManifestLoad with current Manifest.load() signature ([a1f9db1](https://github.com/distsystem/nuefs/commit/a1f9db122d3b12a7f4b18fbeb2a09e7564201ea4))
* upgrade pyo3-stub-gen and adjust fuse handler ([b969b1c](https://github.com/distsystem/nuefs/commit/b969b1c25dc1d77ecff1bc6a9d1d49472cd5c9a4))


### Documentation

* add alternatives comparison table to README ([4ad45af](https://github.com/distsystem/nuefs/commit/4ad45af590c0986b5a93547356000b7404ed0968))
* add CLAUDE.md guidance ([d78137c](https://github.com/distsystem/nuefs/commit/d78137c0d1f2a852f0ddc4305449d19c8fe6b32e))
* replace AGENTS.md with README ([b3083ae](https://github.com/distsystem/nuefs/commit/b3083aed9eabc59ec00f8d02682629fb7c2f922d))


### Code Refactoring

* drop Mapping and accept ManifestEntry lists ([9b4512a](https://github.com/distsystem/nuefs/commit/9b4512a11c1e268bca540f6ab7eb726c606d452f))
* replace ManifestBuilder with Lock compile/save ([05b174e](https://github.com/distsystem/nuefs/commit/05b174e76913c2b57c92af1585b52fde1cd9c65e))
