from datetime import datetime
from typing import Optional
from pydantic import BaseModel

class InternalContact(BaseModel):
    id: str
    first_name: str
    last_name: str
    email: str
    phone: str = ""
    updated_at: datetime


class ExternalContact(BaseModel):
    external_id: Optional[str] = None
    full_name: str = ""
    email_address: str = ""
    phone_number: str = ""
    last_modified: str = ""


class SyncEvent(BaseModel):
    event_id: str
    event_type: str       # "create", "update", "delete"
    internal_id: str
    org_id: str
    object_type: str
    payload: InternalContact
    timestamp: datetime
    crm_type: str  # maybe this can be derived off from somewhere else.