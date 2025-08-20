# Matching Logging System

This document describes the enhanced logging system for the bill reference matching process, which tracks which matching methods worked and which failed for each record.

## Overview

The matching logging system provides detailed insights into the bill reference matching process by:

1. **Tracking every matching attempt** - Each method attempted for each reference is logged
2. **Recording success/failure** - Whether each method succeeded or failed
3. **Storing confidence scores** - Confidence scores for successful matches
4. **Capturing error messages** - Detailed error messages for failed attempts
5. **Logging final results** - Summary of all methods attempted and final outcome

## Files

### Core Components

- `src/streamlined/lobbyist_matching/matcher.py` - Enhanced with `MatchingLogger` class and logging throughout all matching methods
- `analyze_matching_logs.py` - Script to analyze log files and generate performance reports

### Log Files

Log files are automatically created in the `logs/` directory with the naming pattern:
```
logs/matching_results_run_{run_id}.jsonl
```

## Log Format

The log files use JSONL (JSON Lines) format, where each line is a valid JSON object. Each log entry contains:

### Matching Attempt Entries

```json
{
  "timestamp": "2024-01-15T10:30:45.123456",
  "run_id": 123,
  "reference_id": 456,
  "reference_type": "bill",
  "method_attempted": "bill_number_match",
  "success": true,
  "confidence_score": 1.0,
  "matched_bill_id": "hr1234-118",
  "error_message": null,
  "congress_number": 118,
  "bill_type": "hr",
  "bill_number": "1234",
  "title": "Sample Bill Title"
}
```

### Final Result Entries

```json
{
  "timestamp": "2024-01-15T10:30:45.234567",
  "run_id": 123,
  "reference_id": 456,
  "event_type": "final_result",
  "final_match_type": "high_confidence_match",
  "final_confidence": 1.0,
  "final_bill_id": "hr1234-118",
  "methods_attempted": ["bill_number_match"]
}
```

## Matching Methods Tracked

The system tracks the following matching methods:

### Bill Number Matching
- `bill_number_match` - Direct bill number lookup
- `bill_number_match_zero` - Handling for all-zero bill numbers
- `bill_number_fallback_title` - Fallback to title matching for zero bills
- `bill_number_match_success` - Successful bill number match

### Law Number Matching
- `law_number_match` - Law number lookup
- `law_number_match_success` - Successful law number match

### Title Matching
- `title_only_match` - Title-only matching attempts
- `title_only_appropriations` - Appropriations title detection
- `title_only_match_success` - Successful title-only match
- `title_with_bill_prefix_match` - Prefix matching with bill context
- `title_with_bill_exact_match` - Exact title matching with bill context
- `title_with_bill_fuzzy_match` - Fuzzy title matching with bill context

### Special Cases
- `appropriations_title_match` - Special appropriations matching
- `validation` - Reference validation
- `combined_reference_skip` - Skipping combined references
- `unknown_type` - Unknown reference types
- `exception_handling` - Exception handling

## Usage

### Running with Logging

The logging is automatically enabled when running the matching process:

```bash
python src/streamlined/lobbyist_matching/run_pre2008_lda.py --sample-size 1000
```

The script will automatically create a log file and report its location:

```
Starting bill reference matching with detailed logging...
Matching results will be logged to: logs/matching_results_run_123.jsonl
```

### Analyzing Logs

Use the analysis script to generate performance reports:

```bash
# Basic analysis
python analyze_matching_logs.py logs/matching_results_run_123.jsonl

# Save detailed results to JSON file
python analyze_matching_logs.py logs/matching_results_run_123.jsonl --output analysis_results.json
```

## Analysis Output

The analysis script provides:

### Overview Statistics
- Total matching attempts
- Total final results
- Success rates by method

### Final Results Breakdown
- Distribution of match types (high_confidence_match, unmatched, etc.)
- Confidence score distribution

### Method Performance
- Success rate for each matching method
- Number of attempts, successes, and failures
- Top performing methods
- Methods with low success rates

### Example Output

```
============================================================
MATCHING PERFORMANCE ANALYSIS
Log file: logs/matching_results_run_123.jsonl
============================================================

📊 OVERVIEW:
  Total matching attempts: 2,450
  Total final results: 1,200

🎯 FINAL RESULTS:
  high_confidence_match: 850 (70.8%)
  unmatched: 320 (26.7%)
  moderate_confidence_match: 30 (2.5%)

📈 CONFIDENCE SCORE DISTRIBUTION:
  1.0 (exact): 600 (50.0%)
  0.7-1.0 (high): 250 (20.8%)
  0.0 (unmatched): 320 (26.7%)
  0.3-0.7 (medium): 30 (2.5%)

🔍 METHOD PERFORMANCE:
  bill_number_match_success:
    Success rate: 95.2%
    Total attempts: 1,200
    Successes: 1,142
    Failures: 58

🏆 TOP PERFORMING METHODS:
  1. bill_number_match_success: 95.2% success rate
  2. law_number_match_success: 92.1% success rate
  3. title_with_bill_exact_match: 88.5% success rate
```

## Benefits

1. **Debugging** - Quickly identify which methods are failing and why
2. **Performance Optimization** - Focus improvements on low-success-rate methods
3. **Quality Assurance** - Monitor matching quality over time
4. **Research** - Analyze patterns in matching failures
5. **Congress Number Tracking** - Ensure congress numbers are properly preserved

## Congress Number Fix

The logging system also helps track the congress number preservation fix:

- **Before**: Unmatched references had `matched_congress = None`
- **After**: Unmatched references preserve `matched_congress = reference.get("congress_number")`

This prevents thousands of bills from automatically getting a confidence score of 0 due to missing congress numbers.

## Troubleshooting

### Log File Not Found
- Ensure the `logs/` directory exists
- Check that the run completed successfully
- Verify the run_id in the filename

### Empty Log File
- Check that the matching process actually ran
- Verify that references were extracted and processed
- Look for errors in the main processing logs

### Performance Issues
- Large log files can be analyzed in chunks
- Use the `--output` option to save analysis results
- Consider compressing old log files

## Future Enhancements

Potential improvements to the logging system:

1. **Real-time monitoring** - Stream logs to a monitoring dashboard
2. **Alerting** - Notify when success rates drop below thresholds
3. **Trend analysis** - Track performance over multiple runs
4. **Machine learning insights** - Use logs to train better matching models
5. **Integration** - Connect with existing monitoring systems
