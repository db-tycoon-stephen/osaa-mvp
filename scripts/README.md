# Scripts Directory

This directory contains utility scripts for managing the UN-OSAA data pipeline project.

## Issue Management

### `issue_manager.py`

Helper script for Claude to manage GitHub issues intelligently.

**Features**:
- List and filter issues by priority, agent, status
- Analyze issue dependencies
- Suggest optimal work order
- Identify which agent should work on each issue
- Track progress across issues

**Usage**:
```bash
# Show issue summary and top 5 work items
python scripts/issue_manager.py

# Show detailed issue summary
python scripts/issue_manager.py --summary

# Show suggested work plan (top 10)
python scripts/issue_manager.py --plan

# Get the next issue to work on
python scripts/issue_manager.py --next

# Filter issues for specific agent
python scripts/issue_manager.py --agent analytics-engineer
python scripts/issue_manager.py --agent data-pipeline-architect

# Show issue dependencies
python scripts/issue_manager.py --dependencies
```

**Integration with Claude**:

Claude's `github-issue-manager` agent uses this script to:
1. Find the next highest priority issue
2. Understand which specialized agent should handle it
3. Check for dependencies before starting work
4. Track progress across multiple issues

**Examples**:

```bash
# Find next critical issue
python scripts/issue_manager.py --next

# Output:
# 🎯 Next Issue to Work On:
#    Issue #1: Missing Comprehensive Testing Framework
#    Suggested Agent: test-automator
#    Priority Score: 120
#
# To start working:
#    gh issue view 1
#    git checkout -b issue-1

# Show all issues needing data pipeline work
python scripts/issue_manager.py --agent data-pipeline-architect

# Output:
# 📋 Issues for data-pipeline-architect:
#    #2: No Incremental Processing Strategy
#    #8: No Partitioning or Clustering Strategy
```

## Future Scripts

Additional scripts to be added:

- `validate_schema.py` - Validate schema compatibility
- `benchmark_pipeline.py` - Run performance benchmarks
- `data_quality_report.py` - Generate data quality reports
- `backup_database.py` - Backup SQLMesh database
- `setup_s3_lifecycle.py` - Configure S3 lifecycle rules

## Development

**Requirements**:
```bash
pip install PyGithub  # For GitHub API access
```

**Environment Variables**:
```bash
export GITHUB_TOKEN=your_token_here  # For API access
```

**Testing Scripts**:
```bash
# Test issue manager
python scripts/issue_manager.py --repo db-tycoon-stephen/osaa-mvp
```

## Integration with Claude Agents

These scripts are designed to be used by Claude's specialized agents:

- **github-issue-manager**: Uses `issue_manager.py` to prioritize and coordinate work
- **data-pipeline-architect**: May use architecture analysis scripts
- **analytics-engineer**: May use schema validation scripts
- **test-automator**: May use test coverage reporting scripts

See `docs/CLAUDE_WORKFLOW.md` for complete workflow documentation.