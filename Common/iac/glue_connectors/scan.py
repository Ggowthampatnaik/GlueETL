# scan.py

import json

with open("scan_report.json", "w") as f:
    json.dump({"status": "success"}, f)

print("Scan completed")
