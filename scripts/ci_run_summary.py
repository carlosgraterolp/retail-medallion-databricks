"""Write a Markdown summary of the latest pipeline run (for $GITHUB_STEP_SUMMARY).

Uses the Databricks CLI with the credentials already in the environment. Prints task states,
durations and each notebook's result line, plus the dbt result line. It never prints URLs,
hosts or identities. Exits non-zero if any task did not succeed.

usage: python3 scripts/ci_run_summary.py --target prod
"""

import argparse
import json
import subprocess
import sys

JOB_KEY = "retail_medallion_pipeline"


def cli(*args):
    out = subprocess.run(["databricks", *args, "-o", "json"], capture_output=True, text=True, check=True)
    return json.loads(out.stdout)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--target", default="prod")
    target = parser.parse_args().target

    job_id = cli("bundle", "summary", "-t", target)["resources"]["jobs"][JOB_KEY]["id"]
    runs = cli("jobs", "list-runs", "--job-id", str(job_id), "--limit", "1")
    runs = runs if isinstance(runs, list) else runs.get("runs", [])
    if not runs:
        print("No run found.")
        return 1
    run = cli("jobs", "get-run", str(runs[0]["run_id"]))

    print("## Pipeline run\n")
    print(f"Result: **{run['state'].get('result_state', run['state'].get('life_cycle_state'))}**\n")
    print("| Task | State | Duration | Result |")
    print("|---|---|---|---|")
    order = ["bronze", "silver", "gold", "quality_checks", "dbt_marts"]
    tasks = sorted(run["tasks"], key=lambda t: order.index(t["task_key"]) if t["task_key"] in order else 99)
    failed = False
    for t in tasks:
        state = t["state"].get("result_state", t["state"].get("life_cycle_state"))
        failed |= state != "SUCCESS"
        seconds = round((t.get("execution_duration") or 0) / 1000)
        detail = ""
        try:
            output = cli("jobs", "get-run-output", str(t["run_id"]))
            if output.get("notebook_output", {}).get("result"):
                detail = output["notebook_output"]["result"]
            elif output.get("logs"):
                lines = [line for line in output["logs"].splitlines() if "Done. PASS=" in line]
                detail = lines[-1].split("Done. ", 1)[-1] if lines else ""
            if output.get("error"):
                detail = output["error"].splitlines()[0][:300]
        except subprocess.CalledProcessError:
            detail = "(output not available)"
        detail = detail.replace("|", "\\|")
        print(f"| {t['task_key']} | {state} | {seconds} s | {detail} |")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
