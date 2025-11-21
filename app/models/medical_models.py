from pydantic import BaseModel
from typing import List, Dict, Optional
from datetime import datetime


class SubstanceManufacturer(BaseModel):
    substance_name: str
    manufacturers: List[str]


class SubstanceConsumer(BaseModel):
    substance_name: str
    preparation_trade_name: str
    preparation_inn_name: str
    preparation_manufacturer: str
    preparation_country: str
    registration_number: str
    registration_date: str
    release_forms: str


class Statistics(BaseModel):
    total_records: int
    substances_found: int
    preparations_found: int
    substance_consumers_found: int
    unique_substances: int
    top_manufacturers: Dict[str, int]
    top_substances: Dict[str, int]
    countries_distribution: Dict[str, int]


class MedicalAnalysisResult(BaseModel):
    timestamp: str
    source_file: str
    statistics: Statistics
    substances_manufacturers: List[SubstanceManufacturer]
    substance_consumers: List[SubstanceConsumer]