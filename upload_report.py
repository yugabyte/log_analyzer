import argparse
import json
import requests

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Upload a log report to the webserver.")
    parser.add_argument("--ticket", required=True, help="Ticket number")
    parser.add_argument("--universe", required=True, help="Universe name")
    parser.add_argument("--file", required=True, help="Path to JSON report file")
    parser.add_argument("--url", default="http://localhost:5000/upload", help="Upload endpoint URL")
    args = parser.parse_args()

    with open(args.file, "r") as f:
        json_report = json.load(f)

    payload = {
        "universe_name": args.universe,
        "ticket": args.ticket,
        "json_report": json_report
    }

    resp = requests.post(args.url, json=payload)
    print(f"Status: {resp.status_code}")
    print(resp.text)
