# Claude + GitHub Issues Workflow

This document explains how to use Claude to work through GitHub issues in the UN-OSAA data pipeline project.

## Quick Start

### Option 1: Simple Command (Recommended)
```bash
cd ~/Projects/claude/osaa-mvp
claude

# Then in Claude:
"Work on the next highest priority issue in this repo"
```

Claude will:
1. Use `scripts/issue_manager.py` to find the next issue
2. Read the issue with `gh issue view`
3. Delegate to appropriate specialized agents
4. Implement the solution
5. Create tests
6. Submit a PR

### Option 2: Specific Issue
```bash
claude

# Then:
"Work on issue #1 - implement the pytest testing framework"
```

### Option 3: Batch Processing
```bash
claude

# Then:
"Work through all critical issues (#1, #2, #3) in priority order"
```

## Agent Collaboration Model

The `github-issue-manager` agent coordinates with specialized data agents to solve issues:

```
┌─────────────────────────────────────────────────────────┐
│         github-issue-manager (Orchestrator)              │
│  • Reads GitHub issues                                   │
│  • Understands requirements                              │
│  • Delegates to specialized agents                       │
│  • Integrates solutions                                  │
│  • Creates PRs                                           │
└─────────────────────────────────────────────────────────┘
                            ↓
        ┌───────────────────┼───────────────────┐
        ↓                   ↓                   ↓
┌───────────────┐  ┌────────────────────┐  ┌──────────────┐
│data-pipeline- │  │analytics-engineer  │  │data-engineer │
│  architect    │  │                    │  │              │
│               │  │ • SQLMesh models   │  │ • Pipeline   │
│ • Architecture│  │ • Dimensional      │  │   code       │
│ • Tool choice │  │   modeling         │  │ • Error      │
│ • Design      │  │ • dbt patterns     │  │   handling   │
└───────────────┘  └────────────────────┘  └──────────────┘
                            ↓
                   ┌────────────────────┐
                   │modern-data-stack-  │
                   │    engineer        │
                   │                    │
                   │ • dlt, DuckDB      │
                   │ • Implementation   │
                   │ • Optimization     │
                   └────────────────────┘
```

## Example Workflows

### Example 1: Testing Framework (Issue #1)

**Issue**: Missing Comprehensive Testing Framework (Critical)

**Claude Workflow**:
```
User: "Work on issue #1"

github-issue-manager:
  1. Reads issue #1 with gh issue view
  2. Identifies: Critical priority, testing category
  3. Understands acceptance criteria:
     - Unit test coverage >70%
     - Integration tests for S3
     - SQLMesh model tests
     - CI/CD integration

  4. Creates plan:
     Step 1: Set up pytest framework
     Step 2: Create unit tests
     Step 3: Create integration tests
     Step 4: Add to CI/CD

  5. Delegates to test-automator agent:
     "Create pytest framework for this data pipeline project.
      Include unit tests for src/pipeline/ and integration tests
      for S3 operations using moto."

  6. test-automator implements:
     - tests/conftest.py
     - tests/unit/test_ingest.py
     - tests/unit/test_s3_sync.py
     - tests/integration/test_s3_operations.py
     - pytest.ini
     - Updates .github/workflows/test.yml

  7. Verifies: Runs pytest to ensure all tests pass

  8. Creates PR:
     git checkout -b issue-1-testing-framework
     git commit -m "Fix #1: Implement comprehensive testing framework"
     gh pr create --title "Fix #1: Add pytest framework with unit and integration tests"

  9. Updates issue:
     gh issue comment 1 --body "✅ Implemented. PR #14 ready for review."
```

### Example 2: Incremental Processing (Issue #2)

**Issue**: No Incremental Processing Strategy (Critical)

**Claude Workflow**:
```
User: "Work on issue #2"

github-issue-manager:
  1. Reads issue #2
  2. Identifies: Data pipeline optimization, SQLMesh expertise needed
  3. Understands: Need to convert FULL refresh to incremental

  4. Delegates to data-pipeline-architect:
     "Review the SDG, OPRI, and WDI datasets in sqlMesh/models/.
      Recommend incremental strategy for each:
      - INCREMENTAL_BY_TIME_RANGE?
      - INCREMENTAL_BY_UNIQUE_KEY?
      - What time column to use?
      - Any partitioning considerations?"

  5. data-pipeline-architect analyzes:
     - Reviews data patterns
     - Checks for timestamp columns
     - Recommends: INCREMENTAL_BY_TIME_RANGE with updated_at

  6. Delegates to analytics-engineer:
     "Implement INCREMENTAL_BY_TIME_RANGE for sources.sdg model
      following the recommendation from data-pipeline-architect.
      Add updated_at tracking and test the incremental logic."

  7. analytics-engineer implements:
     - Modifies sqlMesh/models/sources/sdg/sdg_indicators.py
     - Adds updated_at column tracking
     - Implements incremental logic
     - Tests backfill and incremental runs

  8. Delegates to modern-data-stack-engineer:
     "Optimize the DuckDB queries for incremental processing.
      Ensure partition pruning works correctly."

  9. modern-data-stack-engineer optimizes:
     - Adds partitioning hints
     - Optimizes SQL queries
     - Tests performance improvement

  10. Runs benchmark:
      - Before: 45 minutes
      - After: 3 minutes
      - 93% improvement ✅

  11. Creates PR and updates issue
```

### Example 3: Data Quality Validation (Issue #3)

**Issue**: Lack of Data Quality Validation Framework (Critical)

**Claude Workflow**:
```
User: "Work on issue #3"

github-issue-manager:
  1. Reads issue #3
  2. Identifies: Data quality, SQLMesh audits, validation rules
  3. Understands: Need systematic quality checks

  4. Delegates to analytics-engineer:
     "Design a comprehensive data quality framework for UN indicators.
      What quality checks should we implement?
      - Not null checks for which columns?
      - Value range validations?
      - Uniqueness constraints?
      - Referential integrity?"

  5. analytics-engineer designs:
     Quality Framework:
     - Not null: indicator_id, country_id, year
     - Unique: (indicator_id, country_id, year)
     - Range: year between 1990-2030, values realistic
     - Referential: country codes valid

  6. Delegates to modern-data-stack-engineer:
     "Implement SQLMesh audits for the quality framework designed
      by analytics-engineer. Create audits for:
      - sqlMesh/audits/indicators_not_null.sql
      - sqlMesh/audits/indicators_unique.sql
      - sqlMesh/audits/value_ranges.sql"

  7. modern-data-stack-engineer implements audits

  8. Delegates to data-engineer:
     "Add pre-upload validation in src/pipeline/ingest/run.py
      that validates data quality before writing to S3"

  9. data-engineer adds validation logic

  10. Creates comprehensive PR with all components
```

## Helper Scripts

### Issue Manager
```bash
# See issue summary
python scripts/issue_manager.py --summary

# See work plan
python scripts/issue_manager.py --plan

# Get next issue
python scripts/issue_manager.py --next

# See issues for specific agent
python scripts/issue_manager.py --agent analytics-engineer

# See dependencies
python scripts/issue_manager.py --dependencies
```

### Manual Issue Commands
```bash
# List all issues
gh issue list

# View specific issue
gh issue view 1

# Filter by label
gh issue list --label "critical"

# Filter by state
gh issue list --state open

# Create new issue from template
gh issue create
```

## Issue Templates

The project has specialized issue templates for:

1. **🐛 Bug Report** (`.github/ISSUE_TEMPLATE/bug_report.yml`)
   - Severity levels
   - Component identification
   - Reproduction steps
   - Error logs

2. **✨ Feature Request** (`.github/ISSUE_TEMPLATE/feature_request.yml`)
   - Priority levels
   - Problem statement
   - Proposed solution
   - Acceptance criteria
   - Implementation details

3. **🔍 Data Quality** (`.github/ISSUE_TEMPLATE/data_quality.yml`)
   - Issue type (accuracy, completeness, etc.)
   - Affected dataset
   - SQL to reproduce
   - Proposed validation rules

4. **⚡ Performance** (`.github/ISSUE_TEMPLATE/performance.yml`)
   - Component with performance issue
   - Current vs target metrics
   - Profiling data
   - Optimization approach

## Best Practices

### For Users
1. **Be specific**: "Work on issue #1" is better than "fix the tests"
2. **Trust the agents**: Let github-issue-manager delegate to specialists
3. **Review PRs**: Claude creates PRs, you approve and merge
4. **Provide feedback**: Comment on issues if more info needed

### For Claude
1. **Always read the issue first**: Use `gh issue view` before starting
2. **Delegate to specialists**: Use data agents for their expertise
3. **Follow acceptance criteria**: Issue templates provide clear success criteria
4. **Test thoroughly**: All code changes must include tests
5. **Document decisions**: Comment on issues with approach taken
6. **Link PRs properly**: Use "Closes #N" in commit messages

## Monitoring Progress

### GitHub Project Board (Optional)
```bash
# Create project board
gh project create --title "Production Readiness" --owner db-tycoon-stephen

# Add issues to board
gh project item-add <project-id> --issue 1
```

### Issue Labels
- `critical`, `high`, `medium`, `low` - Priority
- `testing`, `data-quality`, `performance` - Category
- `in-progress` - Currently being worked on
- `blocked` - Waiting on something
- `needs-triage` - Needs review before work starts
- `good-first-issue` - Easy starting point

## Troubleshooting

### "Claude can't access GitHub"
```bash
# Check gh CLI is installed
gh --version

# Check authentication
gh auth status

# Re-authenticate if needed
gh auth login
```

### "Issue templates not showing"
- Templates may take a few minutes to appear after push
- Check `.github/ISSUE_TEMPLATE/` directory exists
- Verify YAML syntax is correct

### "Claude isn't delegating to data agents"
- Ensure agents are in `~/.claude/agents/domain/`
- Check agent descriptions clearly define their expertise
- Explicitly mention agent name if needed: "Delegate to analytics-engineer"

## Advanced Usage

### Working on Multiple Issues
```bash
claude

# Then:
"Review issues #1-5 and create a work plan. Then implement them in
the optimal order, considering dependencies."
```

### Custom Workflows
```bash
# Create labels first
gh label create "data-pipeline" --description "Data pipeline issues"

# Then work on specific category
claude

"Work on all issues labeled 'data-pipeline' that are marked critical"
```

### Integration with CI/CD
```yaml
# .github/workflows/claude-assist.yml
name: Claude Ready

on:
  issues:
    types: [labeled]

jobs:
  notify:
    if: github.event.label.name == 'claude-ready'
    runs-on: ubuntu-latest
    steps:
      - uses: actions/github-script@v7
        with:
          script: |
            github.rest.issues.createComment({
              owner: context.repo.owner,
              repo: context.repo.repo,
              issue_number: context.issue.number,
              body: '🤖 This issue is ready for Claude! Start working on it with: `claude` then "Work on issue #' + context.issue.number + '"'
            })
```

## Next Steps

1. **Try it out**: Start with `python scripts/issue_manager.py --next`
2. **Work an issue**: Pick issue #1 and work through it
3. **Review the PR**: See how Claude structured the solution
4. **Iterate**: Provide feedback and continue with next issues

For questions or issues with this workflow, create a GitHub issue using the appropriate template!