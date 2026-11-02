"""Юніт-тести правил валідації варіанта 5. Запуск: див. README (потрібні змінні середовища для app.config)."""
import pytest
from pydantic import ValidationError

from app.schemas import StudyIn

GOOD = {
    "patient_code": "P-001", "modality": "ct", "slice_count": 320,
    "slice_thickness_mm": 0.625, "device": "Siemens", "study_date": "2025-03-01",
}


def make(**over):
    return StudyIn(**{**GOOD, **over})


def test_valid_and_modality_normalized():
    assert make().modality == "CT"


@pytest.mark.parametrize("n", [0, -1])
def test_slice_count_must_be_positive(n):
    with pytest.raises(ValidationError):
        make(slice_count=n)


@pytest.mark.parametrize("t", [0.49, 0.0, -1, 10.001, 11])
def test_thickness_out_of_range(t):
    with pytest.raises(ValidationError):
        make(slice_thickness_mm=t)


@pytest.mark.parametrize("t", [0.5, 10, 10.0, 0.625, 5])
def test_thickness_boundaries_ok(t):
    assert make(slice_thickness_mm=t).slice_thickness_mm == t


@pytest.mark.parametrize("field,value", [
    ("slice_count", "5"), ("slice_count", 5.5), ("slice_count", True),
    ("slice_thickness_mm", "1.0"), ("slice_thickness_mm", True),
    ("study_date", 20250301), ("study_date", "not-a-date"),
    ("patient_code", ""), ("device", "   "),
])
def test_wrong_types_rejected(field, value):
    with pytest.raises(ValidationError):
        make(**{field: value})


def test_extra_and_client_id_forbidden():
    with pytest.raises(ValidationError):
        StudyIn(**GOOD, id=1)


def test_missing_field():
    d = dict(GOOD); d.pop("device")
    with pytest.raises(ValidationError):
        StudyIn(**d)


@pytest.mark.parametrize("field", ["patient_code", "modality", "device"])
def test_nul_character_rejected(field):
    # PostgreSQL не приймає \u0000 у text; без цієї перевірки було б 500 замість 400
    with pytest.raises(ValidationError):
        make(**{field: "a\u0000b"})
