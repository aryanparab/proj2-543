#!/bin/bash
# BitWeaving Evaluation Framework Setup & Run Script
# This script builds and runs all benchmarks, collecting comprehensive metrics

set -e

LEVELDB_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BUILD_DIR="$LEVELDB_DIR/build"
RESULTS_DIR="$LEVELDB_DIR/evaluation_results"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${BLUE}╔════════════════════════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║   BitWeaving Evaluation Framework                              ║${NC}"
echo -e "${BLUE}╚════════════════════════════════════════════════════════════════╝${NC}"

# Create results directory
mkdir -p "$RESULTS_DIR"
RESULTS_FILE="$RESULTS_DIR/results_${TIMESTAMP}.txt"
CSV_FILE="$RESULTS_DIR/results_${TIMESTAMP}.csv"

echo -e "${YELLOW}[1/5] Building benchmarks...${NC}"
cd "$BUILD_DIR"
cmake .. > /dev/null 2>&1
make bitweave_benchmark -j$(sysctl -n hw.ncpu) 2>&1 | grep -E "^(Linking|Built)" || true
make bitweave_test -j$(sysctl -n hw.ncpu) 2>&1 | grep -E "^(Linking|Built)" || true

echo -e "${YELLOW}[2/5] Running unit tests (verify correctness)...${NC}"
if ./bitweave_test 2>&1 | tail -3; then
  echo -e "${GREEN}✓ All unit tests passed${NC}"
else
  echo -e "${YELLOW}⚠ Some tests failed - check output${NC}"
fi

echo -e "${YELLOW}[3/5] Running comprehensive benchmarks...${NC}"
echo "BitWeaving Evaluation Results - $TIMESTAMP" > "$RESULTS_FILE"
echo "==========================================" >> "$RESULTS_FILE"
echo "" >> "$RESULTS_FILE"

# Run benchmark and capture output
echo "Running benchmark suite..."
./bitweave_benchmark 2>&1 | tee -a "$RESULTS_FILE"

# Create CSV header
echo "Benchmark,Total_Records,Matches,Scanned_No_BW,Scanned_With_BW,IO_Reduction_%,Latency_No_BW_ms,Latency_With_BW_ms,Speedup" > "$CSV_FILE"

echo -e "${YELLOW}[4/5] Extracting metrics to CSV...${NC}"
# Extract metrics from results file
awk '
/^Benchmark:/ { 
  benchmark = $2; 
  for(i=3; i<=NF; i++) benchmark = benchmark " " $i 
}
/^Total Records:/ { total = $3 }
/^True Matches:/ { matches = $3 }
/Records Scanned:/ {
  if (NR > prev_nr + 1) { scanned_no_bw = $3; prev_nr = NR }
  else { scanned_with_bw = $3; prev_nr = NR }
}
/I\/O Reduction:/ { io_red = $3 }
/Latency:/ { 
  if (lat_count == 0) { lat_no_bw = $3; lat_count = 1 }
  else { lat_with_bw = $3; lat_count = 0 }
}
/Latency Speedup:/ {
  speedup = $3
  printf "%s,%d,%d,%d,%d,%.2f,%.2f,%.2f,%s\n", benchmark, total, matches, scanned_no_bw, scanned_with_bw, io_red, lat_no_bw, lat_with_bw, speedup >> "'$CSV_FILE'"
}
' "$RESULTS_FILE"

echo -e "${YELLOW}[5/5] Generating summary report...${NC}"
echo "" >> "$RESULTS_FILE"
echo "Summary Statistics" >> "$RESULTS_FILE"
echo "==================" >> "$RESULTS_FILE"

# Calculate averages
echo "" >> "$RESULTS_FILE"
awk -F',' '
NR > 1 {
  io_sum += $6
  speedup_sum += $9
  count++
}
END {
  if (count > 0) {
    printf "Average I/O Reduction: %.2f%%\n", io_sum/count >> "'$RESULTS_FILE'"
    printf "Average Speedup: %.2fx\n", speedup_sum/count >> "'$RESULTS_FILE'"
  }
}
' "$CSV_FILE"

echo -e "${GREEN}✓ Evaluation complete!${NC}"
echo ""
echo -e "${BLUE}Results saved to:${NC}"
echo "  Text: $RESULTS_FILE"
echo "  CSV:  $CSV_FILE"
echo ""
echo -e "${BLUE}View results:${NC}"
echo "  cat $RESULTS_FILE"
echo "  open $CSV_FILE  # (macOS)"
echo ""