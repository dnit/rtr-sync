import abc

from datetime import datetime

from ..models import InternalContact, ExternalContact


class BaseCRMTransformer(abc.ABC):
    @abc.abstractmethod
    def to_external(self, internal: InternalContact, provider: str) -> ExternalContact:
        ...
    @abc.abstractmethod
    def to_internal(self, external: ExternalContact, internal_id: str) -> InternalContact:
        ...


class CRMTransformer(BaseCRMTransformer):
    def to_external(self, internal: InternalContact) -> ExternalContact:
        return ExternalContact(
            full_name=f"{internal.first_name} {internal.last_name}".strip(),
            email_address=internal.email,
            phone_number=internal.phone,
            last_modified=internal.updated_at.isoformat(),
        )

    def to_internal(self, external: ExternalContact, internal_id: str) -> InternalContact:
        parts = external.full_name.split()
        first = parts[0] if parts else ""
        last = " ".join(parts[1:]) if len(parts) > 1 else ""
        return InternalContact(
            id=internal_id,
            first_name=first,
            last_name=last,
            email=external.email_address,
            phone=external.phone_number,
            updated_at=datetime.now(),
        )