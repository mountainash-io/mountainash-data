# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**mountainash-data** is a Python package that provides unified database connections and dataframe abstractions for multiple backends via Ibis. This CLAUDE.md file is specifically for working within the **docs/hiivmind** documentation and project management system.

## Hiivmind Documentation System

The docs/hiivmind directory implements a structured documentation and project management workflow using a file-based system defined in `/home/nathanielramm/.hiivmind/context-filepaths.yaml`.

### Documentation Structure

```
docs/hiivmind/
├── workflow/{project|feature}/          # Project workflow states
├── architecture/{project|feature}/      # Architecture states  
├── retrospectives/                      # Post-work analysis
│   ├── dev/{project|feature}/          # Development retrospectives
│   ├── testing/{project|feature}/      # Testing retrospectives
│   └── documentation/{project|feature}/ # Documentation retrospectives
├── reviews/                            # Pre-work reviews and planning
│   ├── architecture/{project|feature}/ # Code/architecture reviews
│   ├── testing/{project|feature}/      # Testing reviews
│   ├── docstrings/{project|feature}/   # Internal docstring reviews
│   └── documentation/{project|feature}/ # External documentation reviews
└── requirements/                       # Project planning documents
    ├── {project}/                      # Project-level plans
    └── {feature}/                      # Feature-level plans
```

### Key File Types and Patterns

#### State Files (Current Progress)
- **project_workflow_state_{datetime}.md**: Current project workflow state
- **project_architecture_state_{datetime}.md**: Current project architecture state

#### Review Files (Pre-work Planning)
- **code_review_{review_type}_{phase}_{datetime}.md**: Architecture and code reviews
- **testing_review_{phase}_{datetime}.md**: Testing strategy reviews
- **documentation_review_{phase}_{datetime}.md**: Documentation reviews

#### Retrospective Files (Post-work Analysis)
- **development_retrospective_{phase}_{datetime}.md**: Development retrospectives
- **testing_retrospective_{phase}_{datetime}.md**: Testing retrospectives
- **documentation_retrospective_{phase}_{datetime}.md**: Documentation retrospectives

#### Requirements and Planning Files
- **project_master_plan_{datetime}.md**: Overall project plans
- **project_plan_{phase}_{datetime}.md**: Phase-specific project plans
- **feature_master_plan_{datetime}.md**: Feature development plans
- **feature_plan_{phase}_{datetime}.md**: Feature phase plans

### Naming Conventions

Files use structured naming with placeholders:
- `{project|feature}`: Use either project name or feature name
- `{datetime}`: Use YYYYMMDD format (e.g., 20250802)
- `{phase}`: Development phase (e.g., phase1, phase2, initial, final)
- `{review_type}`: Type of review (e.g., consistency, security, performance)

### Working with Hiivmind Files

When creating or updating documentation:

1. **Use appropriate directory structure** based on the file type and project/feature
2. **Follow naming conventions** with proper datetime stamps
3. **Reference related files** when creating retrospectives or follow-up documents
4. **Maintain chronological order** through datetime stamps
5. **Use search patterns** to find related documentation (e.g., `*.md` patterns)

### Documentation Workflow

1. **Planning Phase**: Create requirement and planning documents
2. **Review Phase**: Conduct pre-work reviews and assessments
3. **Development Phase**: Update workflow and architecture state files
4. **Retrospective Phase**: Create post-work analysis and lessons learned

This system enables comprehensive project documentation, progress tracking, and knowledge management throughout the development lifecycle.