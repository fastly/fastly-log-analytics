"""Leaf base models (no intra-package imports) shared by ``models.common`` and domain models."""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, model_serializer


class PassthroughModel(BaseModel):
    """Read model that tolerates unknown keys (``extra="allow"``)."""

    model_config = ConfigDict(extra="allow")


class WireSafeModel(PassthroughModel):
    """``extra="allow"`` model whose serialization omits never-set fields.

    Nested use inside a model whose route does not pass
    ``response_model_exclude_unset=True`` would otherwise emit every declared
    optional field as ``null``. Dropping unset declared fields here keeps the
    wire payload byte-identical to the dict the producer built.
    """

    @model_serializer(mode="wrap")
    def _drop_unset(self, handler):
        data = handler(self)
        for name in type(self).model_fields.keys() - self.model_fields_set:
            data.pop(name, None)
        return data
