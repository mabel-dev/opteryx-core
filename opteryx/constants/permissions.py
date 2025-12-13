# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
All of the query types supported by sqlparser-rs
"""

PERMISSIONS = {
    "Query",  # SELECT
    "Explain",  # EXPLAIN
    "ShowColumns",  # SHOW COLUMNS
    "ShowCreate",  # SHOW CREATE VIEW
    "ShowVariable",  # SHOW variable (single)
    "ShowVariables",  # SHOW variables (all)
    "ShowFunctions",  # SHOW FUNCTIONS
    "ShowTables",  # SHOW TABLES (metadata)
    "Set",  # Allow setting session variables only (no persistent side effects)
}
