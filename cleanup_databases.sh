#!/bin/bash
# BitWeaving Database Cleanup Script
# Removes all test databases to free up disk space

set -e

echo "╔════════════════════════════════════════════════════════════════╗"
echo "║  BitWeaving Database Cleanup Script                            ║"
echo "║  WARNING: This will DELETE all test databases                  ║"
echo "╚════════════════════════════════════════════════════════════════╝"
echo ""

# Check current disk usage
echo "Current disk usage:"
df -h /tmp | head -2
echo ""

# Ask for confirmation
echo "Databases to be deleted:"
echo "  /tmp/bitweave_*"
echo "  /tmp/test_*"
echo "  /tmp/leveldb_*"
echo ""
read -p "Are you sure you want to delete these? (yes/no): " confirm

if [ "$confirm" != "yes" ]; then
  echo "Cleanup cancelled."
  exit 0
fi

echo ""
echo "Cleaning up databases..."

# Count total size before
BEFORE=$(du -sh /tmp 2>/dev/null | awk '{print $1}')
echo "Total /tmp size before: $BEFORE"

# Remove benchmark databases
rm -rf /tmp/bitweave_*
echo "✓ Removed /tmp/bitweave_* databases"

rm -rf /tmp/test_*
echo "✓ Removed /tmp/test_* databases"

rm -rf /tmp/leveldb_*
echo "✓ Removed /tmp/leveldb_* databases"

# Remove any other test databases
rm -rf /tmp/*db* 2>/dev/null || true
echo "✓ Removed other *db* directories"

echo ""
echo "Cleanup complete!"
echo ""

# Show new disk usage
AFTER=$(du -sh /tmp 2>/dev/null | awk '{print $1}')
echo "Total /tmp size after: $AFTER"
echo ""
df -h /tmp | head -2
echo ""

# Also show home directory usage
echo "Home directory usage:"
du -sh ~/ 2>/dev/null | tail -1
echo ""

echo "✓ Cleanup finished successfully"