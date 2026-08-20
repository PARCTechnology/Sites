import json
from copy import deepcopy

TRACE_FILE = "PARC-TR-004.json"


def load_trace(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def empty_state():
    return {
        "status": "NONE",
        "order_qty": 0,
        "filled_qty": 0,
        "leaves_qty": 0,
        "canceled_qty": 0,
    }


def ensure_order(state, event):
    key = f'{event["symbol"]}:{event["order_id"]}'
    if key not in state:
        state[key] = empty_state()
    return key


def apply_event(state, event):
    key = ensure_order(state, event)
    order = state[key]

    action = event["action"]
    qty = event.get("qty", 0)

    if action == "NEW":
        order["status"] = "OPEN"
        order["order_qty"] = qty
        order["filled_qty"] = 0
        order["leaves_qty"] = qty
        order["canceled_qty"] = 0

    elif action == "PARTIAL_FILL":
        order["filled_qty"] += qty
        order["leaves_qty"] = max(
            order["order_qty"] - order["filled_qty"],
            0
        )
        order["status"] = "PARTIALLY_FILLED"

    elif action == "FILL":
        order["filled_qty"] += qty
        order["leaves_qty"] = max(
            order["order_qty"] - order["filled_qty"],
            0
        )
        order["status"] = "FILLED"

    elif action == "CANCEL":
        order["canceled_qty"] = order["leaves_qty"]
        order["leaves_qty"] = 0
        order["status"] = "CANCELLED"

    else:
        raise ValueError(f"Unknown action: {action}")


def build_oracle(trace):
    state = {}

    for event in trace["canonical_stream"]:
        apply_event(state, event)

    return state


def find_fault(trace):
    faults = trace.get("fault_injections", [])
    if not faults:
        return None
    return faults[0]


def classify_event(event, missing_event, trace):
    dependency_model = trace.get("dependency_model", {})
    shared_constraints = dependency_model.get("shared_constraints", [])

    # Direct same-order dependency
    if (
        event["symbol"] == missing_event["symbol"]
        and event["order_id"] == missing_event["order_id"]
    ):
        return "QUARANTINE", "DIRECT_ORDER_DEPENDENCY"

    # Shared account-level dependency
    for constraint in shared_constraints:
        if constraint.get("type") == "ACCOUNT_RISK_LIMIT":
            if (
                event.get("acct") == missing_event.get("acct")
                == constraint.get("key")
            ):
                return "QUARANTINE", "SHARED_ACCOUNT_RISK_DEPENDENCY"

    return "COMMIT", "INDEPENDENT_DOMAIN"


def run_parc(trace):
    state = {}
    quarantined = []
    classifications = {}

    fault = find_fault(trace)
    missing_seq = fault["target_seq"]

    missing_event = next(
        e for e in trace["canonical_stream"]
        if e["seq"] == missing_seq
    )

    retransmit_after = fault.get(
        "retransmit_after_observed_seq",
        float("inf")
    )

    for event in trace["canonical_stream"]:

        if event["seq"] == missing_seq:
            print(f'[DROP] seq={event["seq"]}')
            continue

        if event["seq"] < missing_seq:
            apply_event(state, event)
            print(f'[COMMIT] seq={event["seq"]}')
            continue

        decision, reason = classify_event(
            event,
            missing_event,
            trace
        )

        classifications[str(event["seq"])] = {
            "decision": decision,
            "reason": reason,
        }

        if decision == "COMMIT":
            apply_event(state, event)
            print(
                f'[COMMIT] seq={event["seq"]} '
                f'reason={reason}'
            )
        else:
            quarantined.append(event)
            print(
                f'[QUARANTINE] seq={event["seq"]} '
                f'reason={reason}'
            )

        if event["seq"] == retransmit_after:
            print(f'[RECOVER] seq={missing_seq}')
            apply_event(state, missing_event)

            for q_event in quarantined:
                print(f'[REPLAY] seq={q_event["seq"]}')
                apply_event(state, q_event)

            quarantined.clear()

    return state, classifications


def main():
    trace = load_trace(TRACE_FILE)

    oracle = build_oracle(trace)
    parc_state, classifications = run_parc(trace)

    expected = trace["expected_behavior"][
        "final_canonical_state"
    ]

    print("\n--- Oracle State ---")
    print(json.dumps(oracle, indent=2))

    print("\n--- PARC State ---")
    print(json.dumps(parc_state, indent=2))

    print("\n--- Expected State ---")
    print(json.dumps(expected, indent=2))

    print("\n--- Classifications ---")
    print(json.dumps(classifications, indent=2))

    print("\n--- Results ---")
    print("Oracle == PARC:", oracle == parc_state)
    print("PARC == Expected:", parc_state == expected)


if __name__ == "__main__":
    main()
