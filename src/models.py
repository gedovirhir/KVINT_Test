from typing import List, Dict, Optional

from pydantic import BaseModel, ConfigDict
import numpy as np
from datetime import datetime
from enum import Enum

from src.enum import ERROR_STATUS, CRITICAL_STATUS, COMPLETE_STATUS, REJECT_STATUS

Phone = List[int]

class Status(Enum):
    ERROR = ERROR_STATUS
    COMPLETE = COMPLETE_STATUS
    REJECT = REJECT_STATUS
    CRITICAL = CRITICAL_STATUS

class CorrelationBase(BaseModel):
    correlation_id: Optional[str] = None

class CorrelationInput(CorrelationBase):
    phones: Phone

class AggregationData(BaseModel):
    model_config = ConfigDict(
        extra='allow',
        arbitrary_types_allowed=True
    )
    phone: int

class CorrelationOutput(CorrelationBase):
    model_config = ConfigDict(
        use_enum_values=True, 
    )
    
    status: Status
    task_received: Optional[datetime] = None
    from_: Optional[str] = 'report_service'
    to: Optional[str] = 'client'
    data: Optional[List[AggregationData]] = None
    total_duration: Optional[float] = None
