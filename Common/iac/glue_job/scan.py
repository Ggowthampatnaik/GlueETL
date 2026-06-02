import json

with open("scan_report.json", "w") as f:
    json.dump({"status": "success", "message": "No custom scan configured"}, f)

print("Scan completed")
