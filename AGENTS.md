# NueFS

## 核心概念

在真实工作区上叠加 layer 文件/目录，形成 union 视图。
Layer 的文件"散落"在真实工作区各处，与真实文件合并显示。

## 文件树示例

### 挂载前

```
真实工作区 ~/project/              layer 源 ~/.layers/nvim/
├── .bashrc                        └── .config/
├── .config/                           └── nvim/
│   ├── fish/                              ├── init.lua
│   │   └── config.fish                    └── plugins/
│   └── nvim/                                  └── lazy.lua
│       └── options.lua
└── scripts/
    └── build.sh
```

### 挂载后（用户看到的合并视图）

```
~/project/                          # FUSE 挂载点
├── .bashrc                         # ← real（不变）
├── .config/
│   ├── fish/
│   │   └── config.fish             # ← real（不变）
│   └── nvim/                       # ← union 目录
│       ├── init.lua                # ← layer（real 没有）
│       ├── options.lua             # ← real（layer 没有）
│       └── plugins/                # ← layer 目录（real 没有）
│           └── lazy.lua            # ← layer
└── scripts/
    └── build.sh                    # ← real（不变）
```

## 语义规则

| 场景 | 行为 |
|------|------|
| layer 有，real 没有 | 显示 layer，读写回流到 layer |
| layer 没有，real 有 | 显示 real，读写回流到 real |
| layer 和 real 都有（文件） | **layer 优先**，遮住 real |
| layer 和 real 都有（目录） | **union 合并**，递归合并子内容 |
| 新建文件（union 目录中） | 写到 **real** |
| 新建文件（纯 layer 目录中） | 写到 **layer** |

### 多 layer 支持

多个 layer 按顺序叠加，先声明的优先级更高：

```python
mounts=[
    nuefs.Mount(target=".config/nvim", source="~/.layers/work/nvim"),   # 优先级 1
    nuefs.Mount(target=".config/nvim", source="~/.layers/personal/nvim"), # 优先级 2
]
```

如果 work 和 personal 都有 `init.lua`，显示 work 的版本。

## 挂载点处理

- target 不存在 → Rust 自动创建空文件/目录
- 卸载后 → 保留创建的挂载点

## 最小 API

```rust
/// 单个挂载配置
pub struct Mount {
    pub target: PathBuf,  // 挂载点（真实工作区中的路径）
    pub source: PathBuf,  // layer 源
}

/// 挂载（非阻塞）
pub fn mount(target: PathBuf, mounts: Vec<Mount>) -> PyResult<MountHandle>;

/// 卸载
pub fn unmount(handle: MountHandle) -> PyResult<()>;

/// 查询路径归属
pub fn which(handle: &MountHandle, path: &str) -> PyResult<OwnerInfo>;
```

## Python 使用示例

```python
import nuefs
from pathlib import Path

handle = nuefs.mount(
    root=Path("~/project"),
    mounts=[
        nuefs.Mount(target=Path(".config/nvim"), source=Path("~/.layers/nvim/.config/nvim")),
        nuefs.Mount(target=Path(".config/fish"), source=Path("~/.layers/fish/.config/fish")),
    ],
)

# 查询归属
info = nuefs.which(handle, ".config/nvim/init.lua")
print(f"Owner: {info.owner}, Path: {info.backend_path}")

# 卸载
nuefs.unmount(handle)
```

## 系统要求

FUSE 挂载需要：
1. 安装 `fuse3` 包
2. 用户在 `fuse` 组中，或者配置 `/etc/fuse.conf`：
   ```
   user_allow_other
   ```
