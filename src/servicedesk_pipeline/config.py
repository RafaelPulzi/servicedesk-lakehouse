"""Carregamento de configuração e resolução de caminhos das camadas.

A configuração vem de ``config/settings.yaml`` e pode ser sobrescrita por
variáveis de ambiente, o que é essencial para rodar o mesmo código em
desenvolvimento, no CI e em produção (Databricks/S3) sem alterar o YAML.

Variáveis de ambiente reconhecidas:
    STORAGE_BACKEND     -> storage.backend          (local | s3)
    TABLE_FORMAT        -> storage.table_format      (delta | parquet)
    LOCAL_ROOT          -> storage.local_root
    S3_BUCKET           -> storage.s3.bucket
    S3_PREFIX           -> storage.s3.prefix
    AWS_REGION          -> storage.s3.region
    TICKETS_PER_RUN     -> ingestion.tickets_per_run
    PG_ENABLED          -> serving.postgres.enabled  (true | false)
    PG_HOST/PG_PORT/PG_DATABASE/PG_USER/PG_PASSWORD/PG_SCHEMA
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

DEFAULT_CONFIG_PATH = Path(__file__).resolve().parents[2] / "config" / "settings.yaml"

# Camadas do lakehouse, na ordem do fluxo.
LAYERS = ("bronze", "silver", "gold")


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "y", "on"}


@dataclass
class Settings:
    """Configuração resolvida do pipeline (YAML + overrides de ambiente)."""

    raw: dict[str, Any] = field(default_factory=dict)

    # --- storage -------------------------------------------------------
    @property
    def backend(self) -> str:
        return os.getenv("STORAGE_BACKEND", self.raw["storage"]["backend"]).lower()

    @property
    def table_format(self) -> str:
        return os.getenv("TABLE_FORMAT", self.raw["storage"]["table_format"]).lower()

    @property
    def local_root(self) -> str:
        return os.getenv("LOCAL_ROOT", self.raw["storage"]["local_root"])

    @property
    def s3_bucket(self) -> str:
        return os.getenv("S3_BUCKET", self.raw["storage"]["s3"]["bucket"])

    @property
    def s3_prefix(self) -> str:
        return os.getenv("S3_PREFIX", self.raw["storage"]["s3"]["prefix"])

    @property
    def s3_region(self) -> str:
        return os.getenv("AWS_REGION", self.raw["storage"]["s3"]["region"])

    # --- ingestion -----------------------------------------------------
    @property
    def raw_format(self) -> str:
        return self.raw["ingestion"]["raw_format"]

    @property
    def tickets_per_run(self) -> int:
        return int(os.getenv("TICKETS_PER_RUN", self.raw["ingestion"]["tickets_per_run"]))

    # --- spark ---------------------------------------------------------
    @property
    def app_name(self) -> str:
        return self.raw["spark"]["app_name"]

    @property
    def spark_master(self) -> str:
        return self.raw["spark"]["master"]

    @property
    def shuffle_partitions(self) -> int:
        return int(self.raw["spark"]["shuffle_partitions"])

    # --- sla -----------------------------------------------------------
    @property
    def sla_hours(self) -> dict[str, int]:
        return {k: int(v) for k, v in self.raw["sla_hours"].items()}

    # --- serving / postgres -------------------------------------------
    @property
    def pg_enabled(self) -> bool:
        return _env_bool("PG_ENABLED", self.raw["serving"]["postgres"]["enabled"])

    @property
    def pg(self) -> dict[str, Any]:
        p = self.raw["serving"]["postgres"]
        return {
            "host": os.getenv("PG_HOST", p["host"]),
            "port": int(os.getenv("PG_PORT", p["port"])),
            "database": os.getenv("PG_DATABASE", p["database"]),
            "user": os.getenv("PG_USER", p["user"]),
            "password": os.getenv("PG_PASSWORD", p["password"]),
            "schema": os.getenv("PG_SCHEMA", p["schema"]),
        }

    @property
    def pg_url(self) -> str:
        p = self.pg
        return (
            f"postgresql+psycopg2://{p['user']}:{p['password']}"
            f"@{p['host']}:{p['port']}/{p['database']}"
        )

    # --- resolução de caminhos ----------------------------------------
    def raw_path(self) -> str:
        return self._path("raw")

    def layer_path(self, layer: str) -> str:
        if layer not in LAYERS:
            raise ValueError(f"Camada desconhecida: {layer!r}. Use uma de {LAYERS}.")
        return self._path(layer)

    def table_path(self, layer: str, table: str) -> str:
        return f"{self.layer_path(layer)}/{table}"

    def _path(self, leaf: str) -> str:
        if self.backend == "s3":
            return f"s3a://{self.s3_bucket}/{self.s3_prefix}/{leaf}"
        root = self.local_root
        return f"{root}/{leaf}"


def load_settings(path: str | Path | None = None) -> Settings:
    """Lê o YAML de configuração e devolve um objeto ``Settings``."""
    cfg_path = Path(path) if path else DEFAULT_CONFIG_PATH
    with open(cfg_path, "r", encoding="utf-8") as fh:
        raw = yaml.safe_load(fh)
    return Settings(raw=raw)
