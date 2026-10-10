#!/usr/bin/env python3
"""Freeze bounded official ANP/IBGE inputs outside Git; no database access."""
import argparse
from datetime import datetime, timezone
import gzip
import hashlib
import io
import json
from pathlib import Path
import urllib.request
from urllib.parse import urlparse

ANP = "https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/arquivos/arquivos-dados-cadastrais-dos-revendedores-varejistas-de-combustiveis-automotivos/dados-cadastrais-revendedores-varejistas-combustiveis-automoveis.csv"
IBGE = "https://servicodados.ibge.gov.br/api/v1/localidades/municipios?orderBy=id"
UF = set("AC AL AP AM BA CE DF ES GO MA MT MS MG PA PB PR PE PI RJ RN RS RO RR SC SP SE TO".split())


def decode_bounded(raw, cap):
    if len(raw) > cap:
        raise ValueError("compressed/input bytes exceed cap")
    if raw.startswith(b"\x1f\x8b"):
        raw = gzip.GzipFile(fileobj=io.BytesIO(raw)).read(cap + 1)
    if len(raw) > cap:
        raise ValueError("decoded bytes exceed cap")
    return raw


def fetch(url, cap):
    request = urllib.request.Request(url, headers={"User-Agent": "Abastevo-source-verification/1"})
    with urllib.request.urlopen(request, timeout=30) as response:
        # Redirects must remain on the same official provider.
        if urlparse(response.url).hostname != urlparse(url).hostname:
            raise ValueError("unexpected source redirect host")
        raw = decode_bounded(response.read(cap + 1), cap)
        meta = {"url": url, "final_url": response.url, "last_modified": response.headers.get("Last-Modified"),
                "fetched_at": datetime.now(timezone.utc).isoformat(), "bytes": len(raw),
                "sha256": hashlib.sha256(raw).hexdigest()}
    return raw, meta


def aliases_from_ibge(raw):
    entries = []
    seen = set()
    for row in json.loads(raw):
        uf = (row.get("microrregiao") or {}).get("mesorregiao", {}).get("UF", {}).get("sigla")
        if not uf:
            uf = row.get("regiao-imediata", {}).get("regiao-intermediaria", {}).get("UF", {}).get("sigla")
        code = str(row["id"])
        if uf not in UF or len(code) != 7 or not code.isascii() or not code.isdigit() or code in seen or not row["nome"]:
            raise ValueError("unusable/duplicate IBGE municipality identity")
        seen.add(code)
        entries.append({"uf": uf, "ibge": code, "aliases": [row["nome"]]})
    if not entries or len(entries) > 10000:
        raise ValueError("IBGE municipality count outside bounded reference")
    entries.sort(key=lambda item: item["ibge"])
    return (json.dumps({"entries": entries}, ensure_ascii=False, sort_keys=True) + "\n").encode()


def run(out):
    # Refuse overwrite: frozen bytes and metadata must travel together.
    out.mkdir(parents=True, exist_ok=False)
    registry, anp_meta = fetch(ANP, 100 << 20)
    ibge, ibge_meta = fetch(IBGE, 8 << 20)
    aliases = aliases_from_ibge(ibge)
    for name, raw in (("registry.csv", registry), ("ibge.json", ibge), ("aliases.json", aliases)):
        (out / name).write_bytes(raw)
    meta = {"anp": anp_meta, "ibge": ibge_meta, "aliases_sha256": hashlib.sha256(aliases).hexdigest(),
            "municipalities": len(json.loads(aliases)["entries"]), "inferred_aliases": 0}
    # No preparation/load manifest exists if download/validation is incomplete.
    (out / "sources.json").write_text(json.dumps(meta, indent=2) + "\n")
    print(json.dumps(meta))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", type=Path, required=True)
    run(parser.parse_args().out)
