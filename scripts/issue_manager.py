#!/usr/bin/env python3
"""
GitHub Issue Manager - Helper script for Claude to manage GitHub issues

This script provides utilities for:
- Listing and filtering issues
- Analyzing issue dependencies
- Suggesting work order
- Tracking progress
- Coordinating with data analytics agents
"""

import json
import subprocess
import sys
from dataclasses import dataclass
from typing import List, Dict, Optional
from datetime import datetime


@dataclass
class Issue:
    """GitHub Issue representation"""
    number: int
    title: str
    state: str
    labels: List[str]
    body: str
    created_at: str
    updated_at: str
    assignees: List[str]

    @property
    def priority(self) -> int:
        """Calculate priority score (higher = more urgent)"""
        score = 0

        # Priority from labels
        if 'critical' in self.labels:
            score += 100
        elif any('🔴' in label or 'CRITICAL' in label for label in self.labels):
            score += 100
        elif any('🟠' in label or 'HIGH' in label for label in self.labels):
            score += 50
        elif any('🟡' in label or 'MEDIUM' in label for label in self.labels):
            score += 25
        elif any('🟢' in label or 'LOW' in label for label in self.labels):
            score += 10

        # Type bonuses
        if 'testing' in self.labels or 'data-quality' in self.labels:
            score += 20  # Foundation work

        if 'blocked' in self.labels:
            score -= 100  # Can't work on it

        if 'in-progress' in self.labels:
            score += 5  # Slight boost to finish what's started

        return score

    @property
    def is_ready(self) -> bool:
        """Check if issue is ready to work on"""
        if self.state != 'open':
            return False
        if 'blocked' in self.labels:
            return False
        if 'needs-triage' in self.labels:
            return False
        if 'needs-info' in self.labels:
            return False
        return True

    @property
    def requires_data_agent(self) -> bool:
        """Check if issue requires data analytics agent expertise"""
        data_keywords = [
            'incremental', 'sqlmesh', 'dbt', 'duckdb', 'motherduck',
            'data-quality', 'performance', 'partitioning', 'transformation',
            'dimensional', 'semantic-layer', 'pipeline', 'etl', 'elt'
        ]

        text = (self.title + ' ' + self.body).lower()
        return any(keyword in text for keyword in data_keywords)

    @property
    def suggested_agent(self) -> str:
        """Suggest which specialized agent should work on this"""
        text = (self.title + ' ' + self.body).lower()

        # Check for specific patterns
        if 'incremental' in text or 'performance' in text or 'partitioning' in text:
            return 'data-pipeline-architect'

        if 'sqlmesh' in text or 'model' in text or 'transformation' in text:
            return 'analytics-engineer'

        if 'dlt' in text or 'ingestion' in text or 'orchestration' in text:
            return 'data-engineer'

        if 'duckdb' in text or 'motherduck' in text or any(tool in text for tool in ['dbt', 'sqlmesh', 'dagster']):
            return 'modern-data-stack-engineer'

        if 'test' in text or 'pytest' in text or 'quality' in text:
            return 'test-automator'

        if 'monitoring' in text or 'observability' in text or 'cloudwatch' in text:
            return 'devops-engineer'

        if 'documentation' in text or 'catalog' in text:
            return 'docs-architect'

        return 'github-issue-manager'


class IssueManager:
    """Manage GitHub issues for the project"""

    def __init__(self, repo: str = "db-tycoon-stephen/osaa-mvp"):
        self.repo = repo

    def get_issues(self, state: str = "open", limit: int = 100) -> List[Issue]:
        """Fetch issues from GitHub"""
        cmd = [
            'gh', 'issue', 'list',
            '--repo', self.repo,
            '--state', state,
            '--limit', str(limit),
            '--json', 'number,title,state,labels,body,createdAt,updatedAt,assignees'
        ]

        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            print(f"Error fetching issues: {result.stderr}", file=sys.stderr)
            return []

        issues_data = json.loads(result.stdout)

        issues = []
        for data in issues_data:
            issue = Issue(
                number=data['number'],
                title=data['title'],
                state=data['state'],
                labels=[label['name'] for label in data['labels']],
                body=data.get('body', ''),
                created_at=data['createdAt'],
                updated_at=data['updatedAt'],
                assignees=[assignee['login'] for assignee in data.get('assignees', [])]
            )
            issues.append(issue)

        return issues

    def suggest_work_order(self, issues: Optional[List[Issue]] = None) -> List[Issue]:
        """Suggest optimal order to work on issues"""
        if issues is None:
            issues = self.get_issues()

        # Filter to ready issues
        ready_issues = [i for i in issues if i.is_ready]

        # Sort by priority
        sorted_issues = sorted(ready_issues, key=lambda i: i.priority, reverse=True)

        return sorted_issues

    def analyze_dependencies(self, issues: Optional[List[Issue]] = None) -> Dict[int, List[int]]:
        """Analyze issue dependencies (mentions of other issues)"""
        if issues is None:
            issues = self.get_issues()

        dependencies = {}

        for issue in issues:
            deps = []
            text = issue.body

            # Find references to other issues (#N)
            import re
            matches = re.findall(r'#(\d+)', text)
            for match in matches:
                dep_num = int(match)
                if dep_num != issue.number:  # Don't include self-references
                    deps.append(dep_num)

            if deps:
                dependencies[issue.number] = deps

        return dependencies

    def get_blocked_issues(self, issues: Optional[List[Issue]] = None) -> List[Issue]:
        """Get issues that are blocked"""
        if issues is None:
            issues = self.get_issues()

        return [i for i in issues if 'blocked' in i.labels]

    def get_issues_needing_agent(self, agent_type: str, issues: Optional[List[Issue]] = None) -> List[Issue]:
        """Get issues that need a specific agent"""
        if issues is None:
            issues = self.get_issues()

        return [i for i in issues if i.suggested_agent == agent_type and i.is_ready]

    def print_issue_summary(self, issues: Optional[List[Issue]] = None):
        """Print a summary of issues"""
        if issues is None:
            issues = self.get_issues()

        print(f"\n📊 Issue Summary for {self.repo}")
        print("=" * 80)

        total = len(issues)
        ready = len([i for i in issues if i.is_ready])
        blocked = len([i for i in issues if 'blocked' in i.labels])
        in_progress = len([i for i in issues if 'in-progress' in i.labels])

        print(f"\nOverview:")
        print(f"  Total Open Issues: {total}")
        print(f"  Ready to Work: {ready}")
        print(f"  In Progress: {in_progress}")
        print(f"  Blocked: {blocked}")

        # Group by priority
        critical = [i for i in issues if any('CRITICAL' in label or '🔴' in label for label in i.labels)]
        high = [i for i in issues if any('HIGH' in label or '🟠' in label for label in i.labels)]
        medium = [i for i in issues if any('MEDIUM' in label or '🟡' in label for label in i.labels)]
        low = [i for i in issues if any('LOW' in label or '🟢' in label for label in i.labels)]

        print(f"\nBy Priority:")
        print(f"  🔴 Critical: {len(critical)}")
        print(f"  🟠 High: {len(high)}")
        print(f"  🟡 Medium: {len(medium)}")
        print(f"  🟢 Low: {len(low)}")

        # Group by agent
        print(f"\nBy Suggested Agent:")
        agent_counts = {}
        for issue in issues:
            agent = issue.suggested_agent
            agent_counts[agent] = agent_counts.get(agent, 0) + 1

        for agent, count in sorted(agent_counts.items(), key=lambda x: x[1], reverse=True):
            print(f"  {agent}: {count}")

    def print_work_plan(self, max_issues: int = 10):
        """Print suggested work plan"""
        issues = self.suggest_work_order()

        print(f"\n🎯 Suggested Work Plan (Top {max_issues})")
        print("=" * 80)

        for i, issue in enumerate(issues[:max_issues], 1):
            priority_emoji = '🔴' if issue.priority >= 100 else '🟠' if issue.priority >= 50 else '🟡' if issue.priority >= 25 else '🟢'
            print(f"\n{i}. {priority_emoji} Issue #{issue.number}: {issue.title}")
            print(f"   Priority Score: {issue.priority}")
            print(f"   Suggested Agent: {issue.suggested_agent}")
            print(f"   Labels: {', '.join(issue.labels)}")

            if issue.assignees:
                print(f"   Assigned to: {', '.join(issue.assignees)}")

    def get_next_issue(self) -> Optional[Issue]:
        """Get the next issue to work on"""
        work_order = self.suggest_work_order()

        # Find first unassigned issue
        for issue in work_order:
            if not issue.assignees:
                return issue

        # If all assigned, return highest priority
        return work_order[0] if work_order else None


def main():
    """CLI interface for issue management"""
    import argparse

    parser = argparse.ArgumentParser(description='GitHub Issue Manager for Claude')
    parser.add_argument('--repo', default='db-tycoon-stephen/osaa-mvp', help='GitHub repository')
    parser.add_argument('--summary', action='store_true', help='Print issue summary')
    parser.add_argument('--plan', action='store_true', help='Print work plan')
    parser.add_argument('--next', action='store_true', help='Get next issue to work on')
    parser.add_argument('--agent', help='Filter issues for specific agent')
    parser.add_argument('--dependencies', action='store_true', help='Show issue dependencies')

    args = parser.parse_args()

    manager = IssueManager(repo=args.repo)

    if args.summary:
        manager.print_issue_summary()

    if args.plan:
        manager.print_work_plan()

    if args.next:
        next_issue = manager.get_next_issue()
        if next_issue:
            print(f"\n🎯 Next Issue to Work On:")
            print(f"   Issue #{next_issue.number}: {next_issue.title}")
            print(f"   Suggested Agent: {next_issue.suggested_agent}")
            print(f"   Priority Score: {next_issue.priority}")
            print(f"\nTo start working:")
            print(f"   gh issue view {next_issue.number}")
            print(f"   git checkout -b issue-{next_issue.number}")
        else:
            print("No issues available to work on!")

    if args.agent:
        issues = manager.get_issues_needing_agent(args.agent)
        print(f"\n📋 Issues for {args.agent}:")
        for issue in issues:
            print(f"   #{issue.number}: {issue.title}")

    if args.dependencies:
        deps = manager.analyze_dependencies()
        print("\n🔗 Issue Dependencies:")
        for issue_num, dep_list in deps.items():
            print(f"   #{issue_num} depends on: {', '.join(f'#{d}' for d in dep_list)}")

    # Default: show summary
    if not any([args.summary, args.plan, args.next, args.agent, args.dependencies]):
        manager.print_issue_summary()
        print()
        manager.print_work_plan(max_issues=5)


if __name__ == '__main__':
    main()