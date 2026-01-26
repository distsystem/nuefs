# NueFS API 设计

## 设计目标

设计一套以 `Handle` 为中心的最小 API 集合。

## 现有 API（7 函数 + 1 方法）

```
mount(root, mounts) → MountHandle
unmount(handle)
unmount_root(root)          # 冗余
status(root=None)
which(handle, path)
which_root(root, path)      # 冗余
update(handle, mounts)
get_manifest(handle)
```

问题：
- `*_root` 变体与对应函数功能重复
- 操作分散为独立函数，而非 Handle 方法
- `get_manifest` 命名冗长

## 提议的 API（2 函数 + 4 方法）

```
┌─────────────────────────────────────────────────────────────┐
│                        模块级别                              │
├─────────────────────────────────────────────────────────────┤
│  open(root) → Handle                                         │
│  status() → list[Handle]                                    │
└─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│                      Handle 方法                             │
├─────────────────────────────────────────────────────────────┤
│  handle.update(entries)                                     │
│  handle.which(path) → OwnerInfo | None                      │
│  handle.close()                                             │
└─────────────────────────────────────────────────────────────┘
```

## API 参考

### 模块函数

#### `open(root) → Handle`

打开或创建指定路径的 NueFS 挂载。

```python
h = nuefs.open("~/.local/nuefs")
h.update([
    ManifestEntry(".config/nvim", "/path/to/nvim-config", is_dir=True),
    ManifestEntry(".config/zsh", "/path/to/zsh-config", is_dir=True),
])
```

**行为：**
- 若 root 已存在挂载：返回对应 Handle
- 若 root 未挂载：创建新挂载（初始 manifest 默认来自当前目录扫描）

#### `status() → list[Handle]`

列出所有活跃的 NueFS 挂载。

```python
for h in nuefs.status():
    print(h.root)
```

### Handle 方法

#### `handle.update(entries)`

运行时替换挂载映射。

```python
handle.update([ManifestEntry(".config/nvim", "/new/nvim-config", is_dir=True)])
```

#### `handle.which(path) → OwnerInfo | None`

查询虚拟路径的所有者。

```python
info = handle.which(".config/nvim/init.lua")
if info:
    print(f"owner: {info.owner}")
    print(f"backend: {info.backend_path}")
```

#### `handle.close()`

卸载并释放资源。

```python
handle.close()
```

## 数据类型

```python
class ManifestEntry:
    virtual_path: str  # 挂载根目录内的相对路径
    backend_path: str  # 实际文件系统路径（绝对路径）
    is_dir: bool

class Handle:
    root: str     # 挂载根路径（只读）

class OwnerInfo:
    owner: str         # "real" 或层级源路径
    backend_path: str  # 实际文件系统路径
```

## 迁移指南

| 旧 API                      | 新 API                      |
|----------------------------|----------------------------|
| `mount(root, mounts)`      | `open(root); handle.update(entries)` |
| `unmount(handle)`          | `handle.close()`           |
| `unmount_root(root)`       | `open(root).close()`       |
| `status()`                 | `status()`                 |
| `status(root)`             | `open(root)`               |
| `which(handle, path)`      | `handle.which(path)`       |
| `which_root(root, path)`   | `open(root).which(path)`   |
| `update(handle, mounts)`   | `handle.update(entries)`   |
| `get_manifest(handle)`     | (removed)                  |

## IPC 协议变更

无需变更。内部协议保持不变：

```
Request::Mount      → open(root); handle.update(entries)
Request::Unmount    → handle.close()
Request::Status     → status()
Request::Resolve    → open(root)
Request::Which      → handle.which(path)
Request::Update     → handle.update(entries)
Request::GetManifest → (removed)
```

## 总结

| 指标           | 现有    | 提议    |
|----------------|---------|---------|
| 模块函数       | 7       | 2       |
| Handle 方法    | 1       | 4       |
| 总 API 数量    | 8       | 6       |

新设计：
- 消除 `*_root` 冗余
- 将操作逻辑归类到 Handle
- 使用 Pythonic 的属性访问 manifest
- 支持上下文管理器模式
