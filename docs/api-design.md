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
│  open(root, mounts=None) → Handle                           │
│  status() → list[Handle]                                    │
└─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│                      Handle 方法                             │
├─────────────────────────────────────────────────────────────┤
│  handle.manifest → list[Mapping]      # 属性                 │
│  handle.update(mounts)                                      │
│  handle.which(path) → OwnerInfo | None                      │
│  handle.close()                                             │
└─────────────────────────────────────────────────────────────┘
```

## API 参考

### 模块函数

#### `open(root, mounts=None) → Handle`

打开或创建指定路径的 NueFS 挂载。

```python
# 创建新挂载
h = nuefs.open("~/.local/nuefs", [
    Mapping(".config/nvim", "/path/to/nvim-config"),
    Mapping(".config/zsh", "/path/to/zsh-config"),
])

# 连接已有挂载（通过 root 路径）
h = nuefs.open("~/.local/nuefs")
```

**行为：**
- 提供 `mounts`：创建新挂载，若已挂载则报错
- `mounts` 为 None：连接已有挂载，若未找到则报错

#### `status() → list[Handle]`

列出所有活跃的 NueFS 挂载。

```python
for h in nuefs.status():
    print(f"{h.root}: {len(h.manifest)} mappings")
```

### Handle 方法

#### `handle.manifest → list[Mapping]`

返回当前挂载映射的属性。

```python
for m in handle.manifest:
    print(f"{m.target} ← {m.source}")
```

#### `handle.update(mounts)`

运行时替换挂载映射。

```python
handle.update([
    Mapping(".config/nvim", "/new/nvim-config"),
])
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
# 或使用上下文管理器
with nuefs.open(root, mounts) as h:
    ...  # 退出时自动关闭
```

## 数据类型

```python
class Mapping:
    target: str   # 挂载根目录内的相对路径
    source: str   # 源目录的绝对路径

class Handle:
    root: str     # 挂载根路径（只读）

class OwnerInfo:
    owner: str         # "real" 或层级源路径
    backend_path: str  # 实际文件系统路径
```

## 迁移指南

| 旧 API                      | 新 API                      |
|----------------------------|----------------------------|
| `mount(root, mounts)`      | `open(root, mounts)`       |
| `unmount(handle)`          | `handle.close()`           |
| `unmount_root(root)`       | `open(root).close()`       |
| `status()`                 | `status()`                 |
| `status(root)`             | `open(root)`               |
| `which(handle, path)`      | `handle.which(path)`       |
| `which_root(root, path)`   | `open(root).which(path)`   |
| `update(handle, mounts)`   | `handle.update(mounts)`    |
| `get_manifest(handle)`     | `handle.manifest`          |

## 上下文管理器支持

```python
with nuefs.open(root, mounts) as handle:
    handle.which(".config/nvim/init.lua")
# 自动卸载
```

## IPC 协议变更

无需变更。内部协议保持不变：

```
Request::Mount      → open(root, mounts)
Request::Unmount    → handle.close()
Request::Status     → status()
Request::Resolve    → open(root)  # mounts=None
Request::Which      → handle.which(path)
Request::Update     → handle.update(mounts)
Request::GetManifest → handle.manifest
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
