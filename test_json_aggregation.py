import opteryx
import tempfile
import os
import json

# Create test data
test_data = """{"event": {"bytes_processed": 100}}
{"event": {"bytes_processed": 200}}
{"event": {"bytes_processed": 150}}"""

# Write to a temporary JSONL file
with tempfile.TemporaryDirectory() as tmpdir:
    test_file = os.path.join(tmpdir, "test.jsonl")
    with open(test_file, "w") as f:
        f.write(test_data)
    
    # Test the query with JSON extraction and aggregation
    try:
        sql = """SELECT SUM((event ->> 'bytes_processed')::INTEGER) as total_bytes 
                 FROM jsonl_single"""
        
        session = opteryx.session({"file_local": {"dir": tmpdir}})
        result = session.execute(sql)
        print("Query executed successfully!")
        print(result.to_pandas())
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
