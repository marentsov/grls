from pydantic import BaseModel
from typing import List, Dict, Optional, Any
from datetime import datetime


class SubstanceManufacturer(BaseModel):
    substance_name: str
    manufacturers: List[str]
    first_seen_date: Optional[str] = None
    last_seen_date: Optional[str] = None
    is_current: bool = True
    version: int = 1


class SubstanceConsumer(BaseModel):
    substance_name: str
    preparation_trade_name: str
    preparation_inn_name: str
    preparation_manufacturer: str
    preparation_country: str
    registration_number: str
    registration_date: str
    release_forms: str
    first_seen_date: Optional[str] = None
    last_seen_date: Optional[str] = None
    is_current: bool = True
    version: int = 1


class ChangeLog(BaseModel):
    substance_name: str
    change_type: str  # 'added', 'removed', 'modified'
    old_data: Optional[Dict[str, Any]] = None
    new_data: Optional[Dict[str, Any]] = None
    changed_fields: Optional[List[str]] = None
    changed_at: str


class Statistics(BaseModel):
    total_records: int
    substances_found: int
    preparations_found: int
    substance_consumers_found: int
    unique_substances: int
    top_manufacturers: Dict[str, int]
    top_substances: Dict[str, int]
    countries_distribution: Dict[str, int]
    changes_detected: int = 0
    new_substances: int = 0
    removed_substances: int = 0


class MedicalAnalysisResult(BaseModel):
    timestamp: str
    source_file: str
    statistics: Statistics
    substances_manufacturers: List[SubstanceManufacturer]
    substance_consumers: List[SubstanceConsumer]
    changes: List[ChangeLog] = []