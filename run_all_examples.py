"""Run all kubemq-celery examples and report results."""
from __future__ import annotations

import os
import subprocess
import sys
import time

ROOT = os.path.dirname(os.path.abspath(__file__))
os.chdir(ROOT)
os.environ["PYTHONPATH"] = ROOT
os.environ.setdefault("CELERY_BROKER_URL", "kubemq://localhost:50000")
os.environ.setdefault("CELERY_RESULT_BACKEND", "kubemq://localhost:50000")

PYTHON = sys.executable

SKIP_FRAMEWORK = {
    "examples/integrations/django_integration/manage.py",
    "examples/integrations/django_integration/myapp/tasks.py",
    "examples/integrations/django_integration/myapp/views.py",
    "examples/integrations/django_integration/myproject/celery.py",
    "examples/integrations/django_integration/myproject/settings.py",
    "examples/integrations/django_integration/myproject/urls.py",
    "examples/integrations/aiohttp_integration.py",
    "examples/integrations/fastapi_integration.py",
    "examples/integrations/fastapi_websocket_progress.py",
    "examples/integrations/flask_factory_pattern.py",
    "examples/integrations/flask_integration.py",
    "examples/integrations/litestar_integration.py",
    "examples/integrations/starlette_integration.py",
    "examples/testing/pytest_fixtures.py",
}

NEEDS_WORKER = {
    "examples/quickstart/hello_world.py",
    "examples/quickstart/send_and_get_result.py",
    "examples/canvas/chain.py",
    "examples/canvas/group.py",
    "examples/canvas/chord.py",
    "examples/canvas/starmap.py",
    "examples/canvas/chunks.py",
    "examples/canvas/map.py",
    "examples/canvas/chain_of_groups.py",
    "examples/canvas/chord_error_handling.py",
    "examples/canvas/immutable_signatures.py",
    "examples/canvas/complex_workflow.py",
    "examples/error_handling/basic_retry.py",
    "examples/error_handling/exponential_backoff.py",
    "examples/error_handling/custom_retry_policy.py",
    "examples/error_handling/dead_letter_queue.py",
    "examples/error_handling/error_callbacks.py",
    "examples/error_handling/on_failure_callback.py",
    "examples/error_handling/reject_and_requeue.py",
    "examples/error_handling/task_acks_late.py",
    "examples/error_handling/task_time_limit.py",
    "examples/result_backend/store_and_retrieve.py",
    "examples/result_backend/result_expiration.py",
    "examples/result_backend/group_results.py",
    "examples/result_backend/custom_result_serializer.py",
    "examples/result_backend/ignore_result.py",
    "examples/result_backend/result_backend_options.py",
    "examples/result_backend/task_states.py",
    "examples/signals/task_prerun_postrun.py",
    "examples/signals/task_success_failure.py",
    "examples/signals/custom_state_updates.py",
    "examples/signals/before_task_publish.py",
    "examples/serialization/json_serializer.py",
    "examples/serialization/pickle_serializer.py",
    "examples/serialization/content_type_negotiation.py",
    "examples/serialization/custom_serializer.py",
    "examples/routing/dynamic_routing.py",
    "examples/scheduling/countdown_delay.py",
    "examples/scheduling/eta_scheduling.py",
    "examples/scheduling/one_shot_scheduled.py",
    "examples/scheduling/beat_max_delay_warning.py",
    "examples/monitoring/progress_tracking.py",
    "examples/advanced_patterns/task_binding.py",
    "examples/advanced_patterns/task_inheritance.py",
    "examples/advanced_patterns/custom_task_class.py",
    "examples/advanced_patterns/idempotent_tasks.py",
    "examples/advanced_patterns/task_annotations.py",
    "examples/advanced_patterns/benchmark.py",
    "examples/advanced_patterns/multi_app.py",
}

TIMEOUT_WORKER = 30
TIMEOUT_NO_WORKER = 15

results = {"pass": [], "fail": [], "skip": []}


def collect_files():
    files = []
    for root, dirs, fnames in os.walk("examples"):
        dirs[:] = [d for d in dirs if d not in ("__pycache__", "kubernetes")]
        for f in sorted(fnames):
            if f.endswith(".py") and f != "__init__.py":
                files.append(os.path.join(root, f))
    files.sort()
    return files


def purge_queue():
    subprocess.run(
        [PYTHON, "-c", """
import kubemq_celery
from celery import Celery
app = Celery('p', broker='kubemq://localhost:50000')
with app.connection_for_write() as conn:
    ch = conn.channel()
    ch._purge('celery')
"""],
        capture_output=True, timeout=10,
    )


def start_worker(module):
    return subprocess.Popen(
        [PYTHON, "-m", "celery", "-A", module, "worker",
         "--pool=solo", "--loglevel=error",
         "--without-heartbeat", "--without-mingle", "--without-gossip"],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        env=os.environ,
    )


def stop_worker(proc):
    if proc is None:
        return
    proc.terminate()
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=3)


def run_script(path, timeout):
    try:
        r = subprocess.run(
            [PYTHON, path],
            capture_output=True, text=True, timeout=timeout,
            env=os.environ,
        )
        return r.returncode, (r.stderr + r.stdout).strip().split("\n")[-1]
    except subprocess.TimeoutExpired:
        return -1, "TIMEOUT"


def run_with_worker(path):
    mod = path.replace("/", ".").replace(".py", "")
    purge_queue()
    worker = start_worker(mod)
    time.sleep(3)
    try:
        rc, msg = run_script(path, TIMEOUT_WORKER)
        return rc, msg
    finally:
        stop_worker(worker)


def main():
    files = collect_files()
    total = len(files)
    print(f"Found {total} example scripts\n")

    no_worker_files = []
    worker_files = []
    skip_files = []

    for f in files:
        if f in SKIP_FRAMEWORK:
            skip_files.append(f)
        elif f in NEEDS_WORKER:
            worker_files.append(f)
        else:
            no_worker_files.append(f)

    # Phase 1: no-worker scripts
    print(f"=== Phase 1: {len(no_worker_files)} scripts (no worker needed) ===\n")
    for f in no_worker_files:
        rc, msg = run_script(f, TIMEOUT_NO_WORKER)
        if rc == 0:
            print(f"  PASS  {f}")
            results["pass"].append(f)
        elif rc == -1:
            print(f"  TIMEOUT  {f}")
            results["fail"].append((f, "TIMEOUT"))
        else:
            print(f"  FAIL  {f}: {msg[:100]}")
            results["fail"].append((f, msg[:200]))

    # Phase 2: worker scripts
    print(f"\n=== Phase 2: {len(worker_files)} scripts (with per-script worker) ===\n")
    for i, f in enumerate(worker_files, 1):
        print(f"  [{i}/{len(worker_files)}] {f} ... ", end="", flush=True)
        rc, msg = run_with_worker(f)
        if rc == 0:
            print("PASS")
            results["pass"].append(f)
        elif rc == -1:
            print(f"TIMEOUT")
            results["fail"].append((f, "TIMEOUT"))
        else:
            print(f"FAIL: {msg[:80]}")
            results["fail"].append((f, msg[:200]))

    # Phase 3: skipped
    for f in skip_files:
        results["skip"].append(f)

    # Summary
    print(f"\n{'='*60}")
    print(f"RESULTS: {len(results['pass'])} passed, {len(results['fail'])} failed, {len(results['skip'])} skipped / {total} total")
    print(f"{'='*60}")

    if results["fail"]:
        print("\nFAILED:")
        for f, msg in results["fail"]:
            print(f"  {f}: {msg[:120]}")

    if results["skip"]:
        print(f"\nSKIPPED ({len(results['skip'])} framework-dependent):")
        for f in results["skip"]:
            print(f"  {f}")

    return 0 if not results["fail"] else 1


if __name__ == "__main__":
    sys.exit(main())
