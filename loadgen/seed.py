#!/usr/bin/env python3
"""Заносить N записів через API і зберігає їхні ідентифікатори для генератора."""
import argparse
import json
import urllib.request

MODALITIES = ["CT", "MRI", "XR", "US", "PET"]
DEVICES = ["Siemens SOMATOM", "GE Revolution", "Philips Ingenia", "Canon Aquilion"]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", required=True)
    ap.add_argument("--count", type=int, required=True)
    ap.add_argument("--out", required=True)
    a = ap.parse_args()
    ids = []
    for i in range(a.count):
        body = {
            "patient_code": f"SEED-{i:04d}",
            "modality": MODALITIES[i % len(MODALITIES)],
            "slice_count": 50 + i,
            "slice_thickness_mm": [0.625, 1.0, 2.5, 5.0][i % 4],
            "device": DEVICES[i % len(DEVICES)],
            "study_date": f"2025-{1 + i % 12:02d}-{1 + i % 28:02d}",
        }
        req = urllib.request.Request(
            a.url + "/studies", data=json.dumps(body).encode(),
            headers={"Content-Type": "application/json"}, method="POST",
        )
        with urllib.request.urlopen(req, timeout=10) as r:
            assert r.status == 201, r.status
            ids.append(json.load(r)["id"])
    with open(a.out, "w") as f:
        json.dump(ids, f)
    print(f"занесено {len(ids)} записів, ідентифікатори у {a.out}")


if __name__ == "__main__":
    main()
