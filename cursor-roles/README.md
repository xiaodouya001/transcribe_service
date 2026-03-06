# Cursor角色文件目录

此目录包含所有可用的Cursor AI角色定义文件。

## 文件说明

- `dev.md` - Python全栈AI工程师（默认角色）
- `review.md` - 代码审查专家
- `architect.md` - 架构师
- `tester.md` - 测试专家
- `docs.md` - 文档工程师
- `devops.md` - DevOps工程师

## 使用方法

使用项目根目录的 `switch_role.ps1` 脚本来切换角色：

```powershell
.\switch_role.ps1 <角色名>
```

例如：

```powershell
.\switch_role.ps1 review    # 切换到代码审查专家
.\switch_role.ps1 dev       # 切换到Python全栈AI工程师
```

## 添加新角色

1. 在此目录创建新的 `.md` 文件，例如 `custom.md`
2. 按照现有文件的格式编写角色定义
3. 修改 `switch_role.ps1` 脚本，添加新角色到 `ValidateSet` 和 `$roleNames`

## 注意事项

- 所有角色文件使用 `.md` 扩展名
- 文件内容会被复制到项目根目录的 `.cursorrules` 文件
- 切换前会自动备份当前的 `.cursorrules` 文件
