from datetime import date
from typing import Annotated, Any

from pydantic import BaseModel, ConfigDict, Field, StrictInt, field_validator

INT32_MAX = 2_147_483_647  # межа типу integer у PostgreSQL, не бізнес-правило


class StudyIn(BaseModel):
    """Тіло POST і PUT. Службові поля (id, created_at, updated_at) клієнт не передає."""

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    patient_code: str = Field(min_length=1, max_length=64)
    modality: str = Field(min_length=1, max_length=16)
    # Правило варіанта 5: кількість зрізів більша за нуль
    slice_count: StrictInt = Field(gt=0, le=INT32_MAX)
    # Правило варіанта 5: товщина зрізу від 0.5 до 10 мм
    slice_thickness_mm: Annotated[float, Field(strict=True, ge=0.5, le=10, allow_inf_nan=False)]
    device: str = Field(min_length=1, max_length=128)
    study_date: date

    @field_validator("patient_code", "modality", "device")
    @classmethod
    def _no_nul(cls, v: str) -> str:
        # PostgreSQL не зберігає \u0000 у text: без перевірки це був би 500 із бази
        if "\x00" in v:
            raise ValueError("символ \\u0000 недопустимий")
        return v

    @field_validator("modality")
    @classmethod
    def _upper(cls, v: str) -> str:
        return v.upper()

    @field_validator("slice_thickness_mm")
    @classmethod
    def _three_decimals(cls, v: float) -> float:
        if round(v, 3) != v:
            raise ValueError("не більше трьох знаків після коми")
        return v

    @field_validator("study_date", mode="before")
    @classmethod
    def _date_is_string(cls, v: Any) -> Any:
        if not isinstance(v, str):
            raise ValueError("очікується рядок у форматі YYYY-MM-DD")
        return v
