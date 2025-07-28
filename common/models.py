from pydantic import BaseModel, Field
from typing import Literal, Optional

# Message from 1C

class ClientCreatedMessage(BaseModel):
    """Notification about the creation of a new client in 1C."""
    action: Literal["client_created"] = "client_created"
    client_id: str = Field(..., description="Clients unique ID from 1С")
    client_name: str
    email: Optional[str] = None
    phone: Optional[str] = None
    source_system: Literal["1C"] = "1C"

class InvoiceRequestMessage(BaseModel):
    """Request for account creation from 1C."""
    action: Literal["request_invoice"] = "request_invoice"
    invoice_id: str = Field(..., description="Unique account request ID")
    client_id: str
    amount: float
    currency: Literal["RUB", "KZT", "USD"] = "RUB"
    description: Optional[str] = None
    source_system: Literal["1C"] = "1C"

#  Messages from DMS

class ContractStatusUpdateMessage(BaseModel):
    """Updating contract status from DMS."""
    action: Literal["contract_status_update"] = "contract_status_update"
    contract_id: str = Field(..., description="Unique contract ID from DMS")
    client_id: str
    new_status: Literal["active", "expired", "suspended", "pending"]
    service_type: Optional[str] = None
    source_system: Literal["DMS"] = "DMS"

# Messages from CRM

class ClientUpdatedMessage(BaseModel):
    """Updating customer data in CRM."""
    action: Literal["client_updated"] = "client_updated"
    client_id: str
    updated_fields: dict
    source_system: Literal["CRM"] = "CRM"

class TaskCreatedMessage(BaseModel):
    """Creating a task in CRM."""
    action: Literal["task_created"] = "task_created"
    task_id: str = Field(..., description="Unique task ID from CRM")
    client_id: str
    description: str
    assigned_to: Optional[str] = None
    source_system: Literal["CRM"] = "CRM"

# General messages (e.g., confirmations)

class AcknowledgementMessage(BaseModel):
    """General confirmation message."""
    action: Literal["acknowledgement"] = "acknowledgement"
    original_message_id: str # Message ID to which confirmation is given
    status: Literal["success", "failure"]
    error_message: Optional[str] = None
    source_system: str
    destination_system: str
