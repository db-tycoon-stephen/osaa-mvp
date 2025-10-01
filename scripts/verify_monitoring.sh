#!/bin/bash
# Verification script for monitoring implementation

echo "=============================================="
echo "OSAA Monitoring Implementation Verification"
echo "=============================================="
echo ""

errors=0

# Check core modules
echo "Checking core monitoring modules..."
files=(
    "src/pipeline/monitoring.py"
    "src/pipeline/freshness_monitor.py"
    "src/pipeline/alerting.py"
    "src/pipeline/execution_tracker.py"
)

for file in "${files[@]}"; do
    if [ -f "$file" ]; then
        echo "  ✓ $file"
    else
        echo "  ✗ $file (MISSING)"
        ((errors++))
    fi
done

echo ""
echo "Checking utility scripts..."
scripts=(
    "scripts/setup_monitoring.py"
    "scripts/check_freshness.py"
)

for script in "${scripts[@]}"; do
    if [ -f "$script" ]; then
        if [ -x "$script" ]; then
            echo "  ✓ $script (executable)"
        else
            echo "  ⚠ $script (not executable)"
        fi
    else
        echo "  ✗ $script (MISSING)"
        ((errors++))
    fi
done

echo ""
echo "Checking configuration files..."
configs=(
    "monitoring/cloudwatch_dashboard.json"
)

for config in "${configs[@]}"; do
    if [ -f "$config" ]; then
        echo "  ✓ $config"
    else
        echo "  ✗ $config (MISSING)"
        ((errors++))
    fi
done

echo ""
echo "Checking documentation..."
docs=(
    "docs/MONITORING.md"
    "MONITORING_IMPLEMENTATION.md"
)

for doc in "${docs[@]}"; do
    if [ -f "$doc" ]; then
        size=$(wc -c < "$doc" | xargs)
        echo "  ✓ $doc ($size bytes)"
    else
        echo "  ✗ $doc (MISSING)"
        ((errors++))
    fi
done

echo ""
echo "Checking Python imports..."
python3 << 'PYEOF'
import sys
sys.path.insert(0, 'src')

modules = [
    'pipeline.monitoring',
    'pipeline.freshness_monitor',
    'pipeline.alerting',
    'pipeline.execution_tracker'
]

errors = 0
for module in modules:
    try:
        __import__(module)
        print(f"  ✓ {module}")
    except ImportError as e:
        print(f"  ✗ {module} (Import Error: {e})")
        errors += 1

sys.exit(errors)
PYEOF

py_errors=$?
((errors += py_errors))

echo ""
echo "=============================================="
if [ $errors -eq 0 ]; then
    echo "✓ All checks passed! Monitoring implementation is complete."
    exit 0
else
    echo "✗ Found $errors error(s). Please review the output above."
    exit 1
fi
