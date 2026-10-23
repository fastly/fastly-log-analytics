"""Leaf base models (no intra-package imports) shared by ``models.common`` and domain models."""

from __future__ import annotations

from typing import Annotated, Any

from fastapi.encoders import jsonable_encoder
from pydantic import BaseModel, ConfigDict, PlainSerializer, model_serializer


class PassthroughModel(BaseModel):
    """Read model that tolerates unknown keys (``extra="allow"``)."""

    model_config = ConfigDict(extra="allow")

    def __getitem__(self, key: str) -> Any:
        if key in type(self).model_fields:
            return getattr(self, key)
        extra = self.__pydantic_extra__ or {}
        if key in extra:
            return extra[key]
        raise KeyError(key)

    def get(self, key: str, default: Any = None) -> Any:
        try:
            return self[key]
        except KeyError:
            return default

    def __contains__(self, key: object) -> bool:
        if not isinstance(key, str):
            return False
        extra = self.__pydantic_extra__ or {}
        return (
            key in self.model_fields_set
            or key in extra
            or (key in type(self).model_fields and getattr(self, key) is not None)
        )


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


class SchemaColumn(WireSafeModel):
    column: str | None = None
    name: str = ""
    type: str
    min: str | None = None
    max: str | None = None
    approx_unique: int | None = None
    null_percentage: float | None = None
    count: int | None = None


# One cell of a raw-log row: VARCHAR, the integer/float widths and BOOLEAN pass through; TIMESTAMP
# columns arrive already serialized to ISO strings.
ScalarCell = str | int | float | bool | None

# DB/cache-native values (Decimal, tz-aware datetimes, bytes) encode differently under
# pydantic's JSON mode than under ``jsonable_encoder`` (the pre-typing wire path, e.g.
# ``+00:00`` vs ``Z``). These aliases keep the old encoding while the OpenAPI schema
# still advertises the row/dict shape.
JsonRows = Annotated[
    list[dict[str, Any]], PlainSerializer(jsonable_encoder, return_type=list[dict[str, Any]], when_used="json")
]
JsonAny = Annotated[Any, PlainSerializer(jsonable_encoder, when_used="json")]
JsonDict = Annotated[dict[str, Any], PlainSerializer(jsonable_encoder, return_type=dict[str, Any], when_used="json")]
