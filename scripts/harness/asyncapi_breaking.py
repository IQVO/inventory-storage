#!/usr/bin/env python3
"""Breaking-change gate for apis/asyncapi.yaml, the events contract this repo produces.

Consumers pin this spec as a byte-copy (contracts/pinned.json on their develop)
and decode by message `type`, reading named `data` fields. They tolerate
additive change — unknown types are skipped, unknown data keys ignored — but a
producer edit that (1) removes a message from a channel this context publishes,
(2) removes or renames a required `data` field, or (3) narrows a `data` field's
type (fewer accepted values than before) breaks a pinning consumer's decoder.
This gate fails the PR at the source instead of letting the consumers' stale-pin
gates find it after the merge. oasdiff covers the REST spec; AsyncAPI has no
oasdiff equivalent, hence this ~100-line check (ADR 0043 consumers: exception-
management, inbound-receiving, slotting-optimization; plus this repo's self-pin).

Scope: channels whose operation is `subscribe` — this catalog's convention for
the topics THIS context publishes. Foreign topics it consumes use `publish` and
belong to their producers' gates.

Usage: asyncapi_breaking.py BASE HEAD
       asyncapi_breaking.py --self-test
Exit 0 = compatible, 1 = breaking changes found, 2 = could not judge (never a silent pass).
"""
import sys

try:
    import yaml
except ImportError:  # pragma: no cover - CI installs PyYAML before running this
    sys.exit("PyYAML is missing: python3 -m pip install --require-hashes -r .github/requirements/chart-tests.txt")

PINNERS = ("exception-management", "inbound-receiving", "slotting-optimization",
           "inventory-storage (self-pin: kafka + mcp contract testdata)")
# integer values are also number values; number -> integer therefore narrows
WIDENS = {"number": {"number"}, "integer": {"integer", "number"}}


def load(path):
    with open(path) as fh:
        return yaml.safe_load(fh)


def schemas_of(doc):
    return (doc.get("components") or {}).get("schemas") or {}


def deref(doc, node):
    return schemas_of(doc).get(node["$ref"].rsplit("/", 1)[-1], {}) if "$ref" in node else node


def message_names(doc, channel):
    """Message component names a channel offers, following oneOf $refs."""
    msg = (channel.get("subscribe") or {}).get("message") or {}
    for ref in (msg.get("oneOf") or [msg]):
        if isinstance(ref, dict) and "$ref" in ref:
            yield ref["$ref"].rsplit("/", 1)[-1]


def data_schema(doc, message):
    """A message's `data` subschema: payload $ref resolved, allOf branches merged."""
    m = ((doc.get("components") or {}).get("messages") or {}).get(message) or {}
    payload = deref(doc, m.get("payload") or {})
    props = {}
    for branch in [payload] + [b for b in payload.get("allOf") or [] if isinstance(b, dict)]:
        props.update((deref(doc, branch).get("properties") or {}))
    return props.get("data") or {}


def narrowing(old, new):
    """'' when `new` accepts everything `old` accepted, else a short why-not."""
    def types(s):
        t = s.get("type")
        return set(t) if isinstance(t, list) else ({t} if t else set())
    ot, nt = types(old), types(new)
    if ot and nt and not nt <= set().union(*(WIDENS.get(t, {t}) for t in ot)):
        return f"type {sorted(ot)} -> {sorted(nt)}"
    oenum, nenum = old.get("enum"), new.get("enum")
    if nenum is not None and (oenum is None or set(map(str, nenum)) < set(map(str, oenum))):
        return "enum values dropped (or enum added to a free-form field)"
    for key, tighter in (("minimum", lambda a, b: b > a), ("minLength", lambda a, b: b > a),
                         ("maximum", lambda a, b: b < a), ("maxLength", lambda a, b: b < a)):
        if key in old and key in new and tighter(old[key], new[key]):
            return f"{key} {old[key]} -> {new[key]}"
    return ""


def breaking(base, head):
    """Findings: strings, one per breaking shape (channel/message/field named)."""
    found = []
    for name, chan in (base.get("channels") or {}).items():
        if "subscribe" not in (chan or {}):
            continue  # a topic this context consumes; its producer's gate owns it
        if name not in (head.get("channels") or {}):
            found.append(f"channel {name}: REMOVED (the topic pinning consumers read is gone)")
            continue
        head_msgs = set(message_names(head, head["channels"][name]))
        for msg in message_names(base, chan):
            if msg not in head_msgs:
                found.append(f"{name}/{msg}: message REMOVED (its CloudEvents type no longer reaches consumers)")
                continue
            b, h = data_schema(base, msg), data_schema(head, msg)
            for field in b.get("required") or []:
                if field not in (h.get("properties") or {}):
                    found.append(f"{name}/{msg}.data.{field}: required data field REMOVED or RENAMED")
            for field, old in (b.get("properties") or {}).items():
                new = (h.get("properties") or {}).get(field)
                if new is not None and (why := narrowing(old, new)):
                    found.append(f"{name}/{msg}.data.{field}: TYPE NARROWED ({why})")
    return found


def _mini(payload_data):
    """A one-channel, one-message doc just rich enough to exercise the checks."""
    msg = {"payload": {"allOf": [
        {"$ref": "#/components/schemas/CloudEventBase"},
        {"type": "object", "required": ["type", "data"], "properties": {
            "type": {"const": "t"}, "data": payload_data}}]}}
    return {"channels": {"warehouse.inventory.events": {"subscribe": {"message": {
        "oneOf": [{"$ref": "#/components/messages/M"}]}}}},
        "components": {"messages": {"M": msg}, "schemas": {
            "CloudEventBase": {"type": "object", "properties": {"id": {"type": "string"}}}}}}


def self_test():
    base = _mini({"type": "object", "required": ["sku", "qty"],
                  "properties": {"sku": {"type": "string"},
                                 "qty": {"type": "integer", "minimum": 1},
                                 "reason": {"type": "string", "enum": ["A", "B"]}}})
    cases = {
        "removed message": {**_mini({"type": "object", "properties": {}}),
                            "channels": {"warehouse.inventory.events": {"subscribe": {"message": {"oneOf": []}}}}},
        "removed required field": _mini({"type": "object", "required": ["sku"], "properties": {"sku": {"type": "string"}}}),
        "narrowed type": _mini({"type": "object", "required": ["sku", "qty"], "properties": {
            "sku": {"type": ["string", "integer"]}, "qty": {"type": "integer", "minimum": 1}}}),
        "narrowed enum": _mini({"type": "object", "required": ["sku", "qty"], "properties": {
            "sku": {"type": "string"}, "qty": {"type": "integer", "minimum": 2},
            "reason": {"type": "string", "enum": ["A"]}}}),
        "additive only (must stay green)": _mini({"type": "object", "required": ["sku", "qty"], "properties": {
            "sku": {"type": "string"}, "qty": {"type": "number", "minimum": 1},
            "reason": {"type": "string", "enum": ["A", "B"]},
            "extra": {"type": "string"}}}),
    }
    expect_breaking = [k for k in cases if "must stay green" not in k]
    failures = 0
    for label, head in cases.items():
        found = breaking(base, head)
        should = label in expect_breaking
        ok = bool(found) == should and (not should or any(
            w in " | ".join(found) for w in ("REMOVED", "NARROWED")))
        print(f"  {'ok  ' if ok else 'FAIL'} {label}: {found or 'compatible'}")
        failures += not ok
    # a field narrowed number -> integer must trip; integer -> number must not
    n2i = breaking(_mini({"type": "object", "properties": {"qty": {"type": "number"}}}),
                   _mini({"type": "object", "properties": {"qty": {"type": "integer"}}}))
    i2n = breaking(_mini({"type": "object", "properties": {"qty": {"type": "integer"}}}),
                   _mini({"type": "object", "properties": {"qty": {"type": "number"}}}))
    print(f"  {'ok  ' if n2i and not i2n else 'FAIL'} number->integer narrows, integer->number widens")
    return failures == 0 and bool(n2i) and not i2n


def main(argv):
    if argv == ["--self-test"]:
        if not self_test():
            return 1
        print("asyncapi_breaking self-check: PASS")
        return 0
    if len(argv) != 2:
        print(((__doc__ or "") + "\n").split("Usage:")[1].split("Exit")[0].strip(), file=sys.stderr)
        return 2
    try:
        findings = breaking(load(argv[0]), load(argv[1]))
    except (OSError, yaml.YAMLError, AttributeError) as exc:
        print(f"cannot judge {argv[0]} vs {argv[1]}: {exc}", file=sys.stderr)
        return 2
    for f in findings:
        print(f"::error file=apis/asyncapi.yaml::BREAKING: {f}")
    if findings:
        print("::error::apis/asyncapi.yaml carries breaking changes. Pinned byte-copy consumers — "
              + ", ".join(PINNERS) + " — decode these messages by type and field name. "
              "Make the change additive, or version it and refresh every pin (contracts/pinned.json).")
        return 1
    print("asyncapi breaking-change gate: no breaking changes")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
