import os
from datetime import date

def raw_path(source, entity, today=None):
    d = today or str(date.today())
    p = f"data/raw/{source}/{entity}/{d}"
    os.makedirs(p, exist_ok=True)
    return p

def fmt_path(source, entity, today=None):
    d = today or str(date.today())
    p = f"data/formatted/{source}/{entity}/{d}"
    os.makedirs(p, exist_ok=True)
    return p

def usage_path(entity, today=None):
    d = today or str(date.today())
    p = f"data/usage/{entity}/{d}"
    os.makedirs(p, exist_ok=True)
    return p