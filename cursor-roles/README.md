# Cursor Role Files

This directory contains the available Cursor AI role definition files for the repository.

## Files

- `dev.md` - Full-stack multi-stack developer (default role)
- `review.md` - Code review specialist
- `architect.md` - System architect
- `tester.md` - QA and testing specialist
- `docs.md` - Technical documentation engineer
- `devops.md` - DevOps and infrastructure engineer
- `prompt.md` - Prompt engineering specialist
- `python_edu_prompt.md` - Structured teaching prompt for Java-to-Python transition material

## How to switch roles

Use the `switch_role.ps1` script from the repository root:

```powershell
.\switch_role.ps1 <role-name>
```

Examples:

```powershell
.\switch_role.ps1 review
.\switch_role.ps1 dev
```

## Adding a new role

1. Create a new `.md` file in this directory, for example `custom.md`
2. Follow the existing role-file structure
3. Update `switch_role.ps1` so the new role appears in `ValidateSet` and `$roleNames`

## Notes

- All role files use the `.md` extension
- Their contents are copied into the repository root `.cursorrules` file when you switch roles
- The current `.cursorrules` file is backed up automatically before switching
