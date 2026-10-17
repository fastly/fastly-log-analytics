"""Leaf base models (no intra-package imports) shared by ``models.common`` and domain models."""

from __future__ import annotations

from typing import Annotated, Any

from fastapi.encoders import jsonable_encoder
from pydantic import BaseModel, ConfigDict, PlainSerializer, model_serializer


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
        fields = type(self).model_fields
        for name in fields.keys() - self.model_fields_set:
            data.pop(name, None)
            info = fields[name]
            for alias in (info.alias, info.serialization_alias):
                if alias:
                    data.pop(alias, None)
        return data


# DB/cache-native values (Decimal, tz-aware datetimes, bytes) encode differently under
# pydantic's JSON mode than under ``jsonable_encoder`` (the pre-typing wire path, e.g.
# ``+00:00`` vs ``Z``). These aliases keep the old encoding while the OpenAPI schema
# still advertises the row/dict shape.
JsonRows = Annotated[
    list[dict[str, Any]], PlainSerializer(jsonable_encoder, return_type=list[dict[str, Any]], when_used="json")
]
JsonAny = Annotated[Any, PlainSerializer(jsonable_encoder, when_used="json")]
JsonDict = Annotated[dict[str, Any], PlainSerializer(jsonable_encoder, return_type=dict[str, Any], when_used="json")]
