from typing import Union

from pydantic import BaseModel, Field


class SourceSchemaElementTypeConfig(BaseModel):
    class Config:
        extra = "forbid"

    type: str | None = None
    # given class as string to remove circular dependency
    fields: list["SourceSchemaFieldsConfig"] | None = None


class SourceSchemaTypeConfig(BaseModel):
    class Config:
        extra = "forbid"

    containsNull: bool | None = None
    elementType: SourceSchemaElementTypeConfig | None = None
    fields: list["SourceSchemaFieldsConfig"] | None = None
    # Union funtion is ussed to accept any of the mentioned types
    type: Union[str, "SourceSchemaTypeConfig"] | None = None


class SourceSchemaFieldsConfig(BaseModel):
    class Config:
        extra = "forbid"

    metadata: None = None
    name: str | None = None
    nullable: bool | None = None
    type: Union[str, SourceSchemaTypeConfig] | None = None


class SourceSchemaConfig(BaseModel):
    class Config:
        extra = "forbid"

    type: str | None = None
    fields: list[SourceSchemaFieldsConfig] | None = None


class CastFieldsConfig(BaseModel):
    class Config:
        extra = "forbid"

    column_name: str | None = None
    data_type: str | None = None


# CHG Addded new class to handle cast columns
class CastConfig(BaseModel):
    class Config:
        extra = "forbid"

    columns: list[CastFieldsConfig] | None = None


# CHG Added new class to handle vacuum
class VacuumConfig(BaseModel):
    class Config:
        extra = "forbid"

    enabled: bool | None = None
    retention_hours: int | None = None
    retention_check: bool | None = None


class SourceTablesConfig(BaseModel):
    class Config:
        extra = "forbid"

    source_table_folder: str
    confidentiality: str | None = None
    streaming: bool | None = None
    add_file_name_column_to_output: bool | None = None
    alt_target_table_name: str | None = None
    schema: SourceSchemaConfig | None = None
    use_external_table: bool | None = None
    enabled: bool | None = None
    full_load: bool | None = None
    root_source: str | None = None
    cast_columns: CastConfig | None = None
    vacuum: VacuumConfig | None = None
    compaction: bool | None = None


class SourceWriteOptionsConfig(BaseModel):
    class Config:
        extra = "forbid"

    mergeSchema: bool | None = None


class SourceReadOptionsConfig(BaseModel):
    class Config:
        extra = "forbid"

    # Used alias to accept that as alternative to the defined name
    # (throwing errors as '.' present in the parameter name)
    cloudFiles_format: str = Field(alias="cloudFiles.format", default=None)
    cloudFiles_includeExistingFiles: bool = Field(
        alias="cloudFiles.includeExistingFiles", default=None
    )


class SourceTablesPrefixConfig(BaseModel):
    class Config:
        extra = "forbid"

    dev: str | None = None
    tst: str | None = None
    pre: str | None = None
    prd: str | None = None


class SourceConfig(BaseModel):
    class Config:
        extra = "forbid"

    name: str
    read_options: SourceReadOptionsConfig | None = None
    write_options: SourceWriteOptionsConfig | None = None
    enabled: bool | None = None
    confidentiality: str | None = None
    full_load: bool | None = None
    tables: list[SourceTablesConfig]
    root_source: str | None = None
    source_table_folder_prefixes: SourceTablesPrefixConfig | None = None
    vacuum: VacuumConfig | None = None
    compaction: bool | None = None


class SourcesConfig(BaseModel):
    class Config:
        extra = "forbid"

    sources: list[SourceConfig]


# Used model_rebuild() funtion to remove circular dependency
SourceSchemaFieldsConfig.model_rebuild()
SourceSchemaElementTypeConfig.model_rebuild()
SourceSchemaTypeConfig.model_rebuild()
