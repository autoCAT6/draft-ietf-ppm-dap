# Copyright (c) 2026 Yanzhi Xin
# SPDX-License-Identifier: BSD-2-Clause
"""Observable association invariants and negative-control sensitivity."""

import unittest
from pathlib import Path

from model import LeaderModel, concurrent, run_suite, schedules


class AssociationTests(unittest.TestCase):
    def test_lost_response_reuses_the_committed_job_and_batch(self):
        leader = LeaderModel()
        self.assertIsNone(leader.create("t", b"k", b"request", "after_commit_before_response"))
        response = leader.create("t", b"k", b"request")
        self.assertEqual((response["job"], response["batch"]), ("J1", "B1"))
        self.assertEqual(len(leader.allocated), 1)

    def test_completed_or_deleted_job_never_rebinds_in_reference_policy(self):
        for state in ("complete", "deleted"):
            with self.subTest(state=state):
                leader = LeaderModel()
                initial = leader.create("t", b"k", b"request")
                leader.transition(state)
                retry = leader.create("t", b"k", b"request")
                self.assertEqual(retry["kind"], "associated_error")
                self.assertEqual((retry["job"], retry["batch"]),
                                 (initial["job"], initial["batch"]))
                self.assertEqual(len(leader.allocated), 1)

    def test_changed_payload_is_not_silently_rebound(self):
        leader = LeaderModel()
        leader.create("t", b"k", b"request")
        response = leader.create("t", b"k", b"different")
        self.assertEqual(response["kind"], "reference_policy_conflict")
        self.assertEqual(leader.mapping[("t", b"k")].payload, b"request")
        self.assertEqual(len(leader.allocated), 1)

    def test_same_key_in_distinct_tasks_does_not_alias(self):
        leader = LeaderModel()
        one = leader.create("t1", b"k", b"request")
        two = leader.create("t2", b"k", b"request")
        self.assertNotEqual(one["job"], two["job"])

    def test_key_length_boundaries(self):
        for size in (0, 256):
            with self.subTest(size=size), self.assertRaises(ValueError):
                LeaderModel().create("t", b"k" * size, b"request")
        for size in (1, 255):
            with self.subTest(size=size):
                self.assertEqual(LeaderModel().create("t", b"k" * size, b"request")["kind"],
                                 "created")

    def test_atomic_reference_survives_every_enumerated_interleaving(self):
        orders = list(schedules())
        self.assertEqual(len(orders), 20)
        self.assertEqual(len(set(orders)), 20)
        for order in orders:
            with self.subTest(order=order):
                result = concurrent(order, atomic=True)
                self.assertTrue(result["preserves_single_allocation"])
                self.assertEqual(result["responses"]["A"], result["responses"]["B"])

    def test_concurrent_probe_detects_racy_negative_control(self):
        result = concurrent("ABABAB", atomic=False)
        self.assertFalse(result["preserves_single_allocation"])
        self.assertNotEqual(result["responses"]["A"], result["responses"]["B"])
        self.assertTrue(concurrent("AAABBB", atomic=False)["preserves_single_allocation"])

    def test_crash_probe_distinguishes_atomic_and_split_persistence(self):
        atomic = LeaderModel()
        split = LeaderModel()
        atomic.create("t", b"k", b"r", "after_commit_before_response")
        split.create("t", b"k", b"r", "after_job_before_mapping")
        atomic.create("t", b"k", b"r")
        split.create("t", b"k", b"r")
        self.assertEqual(len(atomic.allocated), 1)
        self.assertEqual(len(split.allocated), 2)

    def test_retention_probe_detects_lost_association(self):
        leader = LeaderModel()
        leader.create("t", b"k", b"r")
        leader.transition("deleted", forget_mapping=True)
        self.assertEqual(leader.create("t", b"k", b"r")["job"], "J2")

    def test_manifest_outcomes_and_concurrency_controls(self):
        result, traces = run_suite(Path(__file__).parent)
        self.assertTrue(result["all_model_expectations_met"])
        self.assertEqual(result["concurrency"]["atomic_reference"]["duplicate_allocation"], 0)
        self.assertGreater(result["concurrency"]["racy_negative_control"]["duplicate_allocation"], 0)
        self.assertEqual(len(traces), len(result["sequential"]) + 40)


if __name__ == "__main__":
    unittest.main()
