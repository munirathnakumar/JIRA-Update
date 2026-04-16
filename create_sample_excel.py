"""Run this once to generate a sample input.xlsx for testing."""
import pandas as pd

data = {
    "JIRA_ID":      ["PROJECT-101", "PROJECT-102", "PROJECT-103"],
    "Summary":      ["Fix login bug", "Add export feature", ""],
    "Description":  ["Users cannot log in on mobile", "Export to CSV required", "No change"],
    "Priority":     ["High", "Medium", "Low"],
    "Assignee":     ["user@example.com", "", "user2@example.com"],
    "Labels":       ["bug,mobile", "feature", ""],
    "Story Points": ["3", "5", "2"],
}

df = pd.DataFrame(data)
df.to_excel("input.xlsx", index=False)
print("Created input.xlsx")

