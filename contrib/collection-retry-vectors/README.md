# Collection-job retry design probes

Executable, implementation-independent traces supporting technical discussion of [DAP #795](https://github.com/ietf-wg-ppm/draft-ietf-ppm-dap/issues/795) and proposed [PR #797](https://github.com/ietf-wg-ppm/draft-ietf-ppm-dap/pull/797), pinned at `1af12a40e7e72bedfe55ac9bfeb8573433ccd25d`.

The original lost-response problem and the query-key proposal come from the existing author discussion. This supplementary package does not implement DAP, reproduce a Janus defect, or establish protocol conformance. Its presence on this fork does not indicate working-group adoption.

## Run

Python 3.10+ and the standard library are sufficient; no installation or network is required:

```sh
sh run.sh
```

The runner executes 11 tests, then rewrites `results.json` and `traces.jsonl`. To place generated output elsewhere:

```sh
python3 model.py --out /tmp/dap-retry-results
```

`cases.json` contains 13 sequential probes. The model also enumerates the 20 order-preserving interleavings of two three-step workers. In the reference transaction model all 20 keep one committed job; the deliberately racy lookup/allocate/publish control creates two committed jobs in 18. These are counts in a tiny deterministic model, not probabilities or a distributed-systems proof.

## Contract and explicit assumptions

The proposed key is a 1–255-byte `LeaderSelectedQueryConfig` field inside `Query.config`, not an HTTP header. Associations are scoped to `(task, key)`. Request payloads and all job/batch identifiers in these files are synthetic. The opaque `payload` stands for the complete encoded request; this is not a wire decoder.

The observed property is that a recognized key does not silently select another batch/job. Internal temporary candidate allocations in a real implementation are outside this observation. The model makes these additional reference-design choices:

- Known key with changed payload: reject without rebinding; no HTTP status or particular DAP error is asserted.
- Completed/deleted job: return an associated error. This follows one reading of #797's parenthetical; its precedence relative to §4.6.1's retained-resource response remains a clarification question.
- Durable atomic binding: assumed in the reference model, not a mandated storage mechanism. A crash is an injected trace boundary in the same in-memory object, not actual disk/process recovery.
- Deleted job with retained key association: a reference tombstone policy. Forgetting the association is a separate lifecycle probe because #797 does not specify its retention period.

The unkeyed next-batch control illustrates #795's already-reported ambiguity; it is not a conforming implementation of all draft-19 rules. Racy and split-write controls are intentionally faulty implementation choices. The retention probe concerns an unspecified boundary and is not a confirmed protocol violation.

Task/result expiry, GET polling, valid aggregation parameters, extension parsing, actual wire encoding, HTTP, HPKE and VDAF are outside this harness. It cannot determine whether any real implementation is interoperable, safe, or durable.

`retained-completed-resource-first-proposal` exercises the alternative proposed clarification: a retained completed resource returns its current state before overlap rejection. This is an explicit switch, not an assertion that the proposal is adopted. The default interpretation remains present so the two outcomes can be inspected separately; both preserve the original association.

## Questions supported by the traces

1. When a byte-identical POST matches a retained completed collection job, should the existing-resource response precede `batchOverlap` validation? The general rule requires the existing location/current state, whereas #797's parenthetical mentions an already-collected error.
2. If a known key accompanies a different complete `CollectionJobReq`, should implementations compare the original request and reject the mismatch? Which existing error applies?
3. How should key-association retention differ from result retention/deletion while retries remain supported? No permanent retention after task expiry is proposed.

Source wording: [§3.2 resource creation and §4.6.1 collection initialization](https://github.com/ietf-wg-ppm/draft-ietf-ppm-dap/blob/1af12a40e7e72bedfe55ac9bfeb8573433ccd25d/draft-ietf-ppm-dap.md), and the proposed leader-selected query configuration in §5.2.1. [Published draft-19](https://www.ietf.org/archive/id/draft-ietf-ppm-dap-19.txt) is an Internet-Draft, not an RFC.

Code in this directory is provided under the accompanying BSD-2-Clause license. Contributions to the parent repository remain subject to its [contribution guidance](../../CONTRIBUTING.md).
