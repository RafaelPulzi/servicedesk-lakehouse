"""Pipeline de dados (lakehouse) para analytics de chamados de Service Desk.

Arquitetura medallion: raw -> bronze -> silver -> gold -> Postgres -> dashboard.
"""

__version__ = "1.0.0"
