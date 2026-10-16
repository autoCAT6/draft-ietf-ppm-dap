#!/usr/bin/env python3
# Copyright (c) 2026 Yanzhi Xin
# SPDX-License-Identifier: BSD-2-Clause
"""Deterministic design probes for DAP PR #797, not a DAP implementation.

All request bytes and identifiers are synthetic. There is no HTTP, VDAF, HPKE,
Janus execution, or privacy/security proof here. Atomic persistence and the
conflicting-payload response are explicit reference-design choices. A completed
job's associated error is one reading of #797, with precedence still unresolved.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
import itertools
import json
from pathlib import Path
import unittest


@dataclass
class Record:
    task: str
    key: bytes
    payload: bytes
    job: str
    batch: str
    state: str = "pending"


class LeaderModel:
    """Small sequential reference design with optional broken controls."""

    def __init__(self, keyed: bool = True, existing_resource_first: bool = False):
        self.keyed = keyed
        self.existing_resource_first = existing_resource_first
        self.mapping: dict[tuple[str, bytes], Record] = {}
        self.allocated: list[Record] = []
        self.trace: list[dict] = []

    def log(self, event: str, **fields):
        self.trace.append({"event": event, **fields})

    def allocate(self, task: str, key: bytes, payload: bytes) -> Record:
        n = len(self.allocated) + 1
        record = Record(task, key, payload, f"J{n}", f"B{n}")
        self.allocated.append(record)
        self.log("durable_allocation", task=task, key=key.hex(), job=record.job,
                 batch=record.batch)
        return record

    def create(self, task: str, key: bytes, payload: bytes,
               crash: str | None = None) -> dict | None:
        if not 1 <= len(key) <= 255:
            raise ValueError("LeaderSelectedQueryConfig key must be 1–255 bytes")
        self.log("create", task=task, key=key.hex(), payload=payload.hex())
        known = self.mapping.get((task, key)) if self.keyed else None
        if known:
            if known.payload != payload:
                # No HTTP status is asserted by this model. The PR does not
                # specify a dedicated error for this ambiguous reuse.
                self.log("reference_policy_reject_changed_payload", job=known.job)
                return {"kind": "reference_policy_conflict", "job": known.job,
                        "batch": known.batch}
            kind = {"pending": "existing", "complete": "associated_error",
                    "deleted": "associated_error"}[known.state]
            if self.existing_resource_first and known.state == "complete":
                kind = "existing_complete"
            self.log("reuse_association", job=known.job, batch=known.batch,
                     state=known.state, response=kind)
            return {"kind": kind, "job": known.job, "batch": known.batch}
        if crash == "before_commit":
            self.log("crash_before_commit")
            return None
        record = self.allocate(task, key, payload)
        if crash == "after_job_before_mapping":
            self.log("negative_control_crash_between_durable_writes", job=record.job)
            return None
        if self.keyed:
            self.mapping[(task, key)] = record
            self.log("persist_association", job=record.job, batch=record.batch)
        if crash == "after_commit_before_response":
            self.log("crash_after_atomic_commit_before_response", job=record.job)
            return None
        self.log("response", job=record.job, batch=record.batch)
        return {"kind": "created", "job": record.job, "batch": record.batch}

    def transition(self, state: str, forget_mapping: bool = False):
        record = self.allocated[-1]
        record.state = state
        self.log("job_state", job=record.job, state=state)
        if forget_mapping:
            self.mapping.pop((record.task, record.key), None)
            self.log("negative_control_forget_association", job=record.job)


def schedules():
    """Every order preserving interleaving of two three-step workers."""
    for positions in itertools.combinations(range(6), 3):
        chosen = set(positions)
        yield "".join("A" if i in chosen else "B" for i in range(6))


def concurrent(schedule: str, atomic: bool) -> dict:
    """Reference transaction vs deliberately racy lookup/allocate/publish.

    A negative control intentionally omits rechecking at publish time. This
    does not model or establish a bug in any real DAP implementation.
    """
    mapping = None
    allocations = []
    workers = {name: {"pc": 0, "seen": None, "candidate": None, "response": None}
               for name in "AB"}
    trace = []
    for name in schedule:
        w = workers[name]
        step = w["pc"]
        if atomic:
            if step == 0:
                event = "begin"
            elif step == 1:
                if mapping is None:
                    mapping = f"J{len(allocations) + 1}"
                    allocations.append(mapping)
                    event = "atomic_allocate_and_associate"
                else:
                    event = "atomic_reuse"
                w["candidate"] = mapping
            else:
                w["response"] = w["candidate"]
                event = "respond"
        else:
            if step == 0:
                w["seen"] = mapping
                event = "lookup"
            elif step == 1:
                if w["seen"] is None:
                    w["candidate"] = f"J{len(allocations) + 1}"
                    allocations.append(w["candidate"])
                    event = "allocate_without_recheck"
                else:
                    w["candidate"] = w["seen"]
                    event = "reuse_lookup_result"
            else:
                mapping = w["candidate"]
                w["response"] = mapping
                event = "publish_and_respond"
        w["pc"] += 1
        trace.append({"worker": name, "step": step, "event": event,
                      "association": mapping, "allocations": list(allocations)})
    return {"schedule": schedule, "allocations": allocations,
            "responses": {k: v["response"] for k, v in workers.items()},
            "preserves_single_allocation": len(allocations) == 1,
            "trace": trace}


def sequential_case(case: dict) -> dict:
    leader = LeaderModel(keyed=case.get("keyed", True),
                         existing_resource_first=case.get("existing_resource_first", False))
    responses = []
    for action in case["actions"]:
        op = action["op"]
        if op == "create":
            try:
                result = leader.create(action.get("task", "task-1"),
                                       bytes.fromhex(action.get("key", "ab")),
                                       action.get("payload", "request-1").encode(),
                                       action.get("crash"))
            except ValueError:
                result = {"kind": "invalid_key"}
            responses.append(result)
        elif op in ("complete", "delete"):
            leader.transition("complete" if op == "complete" else "deleted",
                              action.get("forget_mapping", False))
        else:
            raise ValueError(f"unknown operation {op}")
    jobs = [r.job for r in leader.allocated]
    expected = case["expected_allocations"]
    return {"id": case["id"], "classification": case["classification"],
            "expected_allocations": expected, "allocated_jobs": jobs,
            "responses": responses, "matches_model_expectation": len(jobs) == expected,
            "trace": leader.trace}


def run_suite(directory: Path) -> tuple[dict, list[dict]]:
    cases = json.loads((directory / "cases.json").read_text())
    sequential = [sequential_case(case) for case in cases["sequential"]]
    interleavings = {}
    traces = []
    for result in sequential:
        traces.append({"case": result["id"], "trace": result.pop("trace")})
    for name, atomic in (("atomic_reference", True), ("racy_negative_control", False)):
        results = [concurrent(s, atomic) for s in schedules()]
        interleavings[name] = {
            "schedules_examined": len(results),
            "single_allocation": sum(r["preserves_single_allocation"] for r in results),
            "duplicate_allocation": sum(not r["preserves_single_allocation"] for r in results),
        }
        for r in results:
            traces.append({"case": name, **r})
    output = {
        "scope": "Synthetic state-machine probes only; no real DAP implementation executed",
        "normative_revision": "PR797 head 1af12a40e7e72bedfe55ac9bfeb8573433ccd25d",
        "sequential": sequential,
        "concurrency": interleavings,
        "all_model_expectations_met": all(r["matches_model_expectation"] for r in sequential),
        "interpretation": {
            "atomicity": "Reference design assumption, not mandated storage architecture",
            "negative_controls": "Racy/split-write controls intentionally defective; retention probe unspecified; none are implementation findings",
            "changed_payload": "Local reject policy; exact protocol response remains a question",
            "completed_response": "Associated error is a reference choice following the parenthetical; relation to existing-resource response remains unresolved",
            "deletion": "Retention comparison identifies a clarification to request, not a proven violation",
            "crash": "Persistence is assumed; retry occurs in the same in-memory object, not an actual process restart",
        },
    }
    return output, traces


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", type=Path, default=Path(__file__).parent)
    args = parser.parse_args()
    suite = unittest.defaultTestLoader.discover(str(Path(__file__).parent), "test_model.py")
    check = unittest.TextTestRunner(verbosity=2).run(suite)
    if not check.wasSuccessful():
        raise SystemExit(1)
    results, traces = run_suite(Path(__file__).parent)
    args.out.mkdir(parents=True, exist_ok=True)
    (args.out / "results.json").write_text(json.dumps(results, indent=2) + "\n")
    (args.out / "traces.jsonl").write_text("".join(json.dumps(t) + "\n" for t in traces))
    print(json.dumps({"tests_run": check.testsRun,
                      "model_expectations_met": results["all_model_expectations_met"],
                      "concurrency": results["concurrency"]}, indent=2))
    if not results["all_model_expectations_met"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
