"""Write a Markdown summary of the latest pipeline run (for $GITHUB_STEP_SUMMARY).

Uses the Databricks CLI with the credentials already in the environment. Prints, per task, the
final attempt's state, duration and result line (notebook exit value or the dbt result line) and
how many retries it took. The Jobs API lists every attempt; a task that failed and then succeeded
on retry counts as succeeded, like the run itself. It never prints URLs, hosts or identities.
Exits non-zero if the run or any task's final attempt did not succeed.

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
    run_state = run["state"].get("result_state", run["state"].get("life_cycle_state"))
    print(f"Result: **{run_state}**\n")
    print("| Task | State | Retries | Duration | Result |")
    print("|---|---|---|---|---|")
    order = ["bronze", "silver", "gold", "quality_checks", "dbt_marts"]
    attempts, retried_after = {}, {}
    for t in run["tasks"]:  # one entry per attempt; keep the latest attempt of each task
        key = t["task_key"]
        if key not in attempts or t.get("attempt_number", 0) > attempts[key].get("attempt_number", 0):
            attempts[key] = t
    for t in run["tasks"]:  # earlier attempts: keep the error class that caused the retry
        key = t["task_key"]
        if t is not attempts[key]:
            try:
                error = cli("jobs", "get-run-output", str(t["run_id"])).get("error") or ""
            except subprocess.CalledProcessError:
                error = ""
            classes = [part.split("]")[0] for part in error.split("[")[1:] if part.split("]")[0].isupper()]
            retried_after[key] = classes[-1] if classes else "error"
    tasks = sorted(attempts.values(), key=lambda t: order.index(t["task_key"]) if t["task_key"] in order else 99)
    failed = run_state != "SUCCESS"
    for t in tasks:
        state = t["state"].get("result_state", t["state"].get("life_cycle_state"))
        retries = t.get("attempt_number", 0)
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
        if t["task_key"] in retried_after:
            detail = f"retried after {retried_after[t['task_key']]}; final attempt: {detail}"
        detail = detail.replace("|", "\\|")
        print(f"| {t['task_key']} | {state} | {retries} | {seconds} s | {detail} |")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
