#!/usr/bin/env python3

import argparse
import csv
import hashlib
import re
from pathlib import Path
from xml.etree import ElementTree

CLDR_VERSION = "48.2"
CLDR_SOURCE_URL = (
    "https://raw.githubusercontent.com/unicode-org/cldr/"
    "release-48-2/common/subdivisions/en.xml"
)
CLDR_SOURCE_SHA256 = (
    "997a14da1144bb66f36a829db1783afe41f7529e33070afbe964bdd8e387b1d2"
)
CLDR_TERRITORY_SOURCE_URL = (
    "https://raw.githubusercontent.com/unicode-org/cldr/"
    "release-48-2/common/main/en.xml"
)
CLDR_TERRITORY_SOURCE_SHA256 = (
    "67607f4c9cf57157e70564987d8ae92a9ddcd73c00a2f1cd6abf79524a969cbd"
)
SUBDIVISION_CODE_PATTERN = re.compile(r"^[a-z]{2}[a-z0-9]+$")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Generate the wremotely country and subdivision alias seeds from "
            f"CLDR {CLDR_VERSION}."
        ),
        epilog=(
            f"Pinned sources: {CLDR_SOURCE_URL} and "
            f"{CLDR_TERRITORY_SOURCE_URL}"
        ),
    )
    parser.add_argument("subdivision_source_xml", type=Path)
    parser.add_argument("territory_source_xml", type=Path)
    parser.add_argument(
        "--countries",
        type=Path,
        default=Path(__file__).with_name("wremotely__countries.csv"),
    )
    parser.add_argument(
        "--subdivision-output",
        type=Path,
        default=Path(__file__).with_name("wremotely__country_subdivisions.csv"),
    )
    parser.add_argument(
        "--country-alias-output",
        type=Path,
        default=Path(__file__).with_name("wremotely__country_cldr_aliases.csv"),
    )
    return parser.parse_args()


def read_subdivisions(source_xml: Path) -> list[tuple[str, str, str]]:
    source_sha256 = hashlib.sha256(source_xml.read_bytes()).hexdigest()
    if source_sha256 != CLDR_SOURCE_SHA256:
        raise ValueError(
            f"{source_xml} does not match the pinned CLDR {CLDR_VERSION} "
            f"source at {CLDR_SOURCE_URL}"
        )

    root = ElementTree.parse(source_xml).getroot()
    language = root.find("./identity/language")
    if language is None or language.attrib.get("type") != "en":
        raise ValueError(f"{source_xml} is not the CLDR English subdivision file")

    rows: list[tuple[str, str, str]] = []
    seen_codes: set[str] = set()
    for element in root.findall("./localeDisplayNames/subdivisions/subdivision"):
        if "draft" in element.attrib:
            continue

        raw_code = element.attrib.get("type", "")
        if SUBDIVISION_CODE_PATTERN.fullmatch(raw_code) is None:
            raise ValueError(f"invalid CLDR subdivision code: {raw_code!r}")

        name = "".join(element.itertext()).strip()
        if not name:
            raise ValueError(f"blank CLDR subdivision name for {raw_code}")

        country_code = raw_code[:2].upper()
        subdivision_code = f"{country_code}-{raw_code[2:].upper()}"
        if subdivision_code in seen_codes:
            raise ValueError(f"duplicate CLDR subdivision code: {subdivision_code}")
        seen_codes.add(subdivision_code)
        rows.append((subdivision_code, country_code, name))

    return sorted(rows)


def write_subdivisions(
    output: Path,
    rows: list[tuple[str, str, str]],
) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, lineterminator="\n")
        writer.writerow(("subdivision_code", "country_code", "subdivision_name"))
        writer.writerows(rows)


def read_country_codes(countries_csv: Path) -> set[str]:
    with countries_csv.open(encoding="utf-8", newline="") as handle:
        rows = csv.DictReader(handle)
        country_codes = {
            str(row["country_code"]).strip()
            for row in rows
            if str(row.get("country_code") or "").strip()
        }
    if len(country_codes) < 200:
        raise ValueError(
            f"expected at least 200 ISO countries in {countries_csv}, "
            f"found {len(country_codes)}"
        )
    return country_codes


def read_country_aliases(
    source_xml: Path,
    country_codes: set[str],
) -> list[tuple[str, str, str]]:
    source_sha256 = hashlib.sha256(source_xml.read_bytes()).hexdigest()
    if source_sha256 != CLDR_TERRITORY_SOURCE_SHA256:
        raise ValueError(
            f"{source_xml} does not match the pinned CLDR {CLDR_VERSION} "
            f"source at {CLDR_TERRITORY_SOURCE_URL}"
        )

    root = ElementTree.parse(source_xml).getroot()
    language = root.find("./identity/language")
    if language is None or language.attrib.get("type") != "en":
        raise ValueError(f"{source_xml} is not the CLDR English locale file")

    rows: set[tuple[str, str, str]] = set()
    for element in root.findall("./localeDisplayNames/territories/territory"):
        country_code = element.attrib.get("type", "")
        if country_code not in country_codes or "draft" in element.attrib:
            continue

        alias = "".join(element.itertext()).strip()
        if not alias:
            raise ValueError(f"blank CLDR territory name for {country_code}")
        match_kind = (
            "exact_code"
            if re.fullmatch(r"[A-Z0-9]{2,3}", alias) is not None
            else "phrase"
        )
        rows.add((country_code, alias, match_kind))

    return sorted(rows)


def write_country_aliases(
    output: Path,
    rows: list[tuple[str, str, str]],
) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, lineterminator="\n")
        writer.writerow(("country_code", "alias", "match_kind"))
        writer.writerows(rows)


def main() -> None:
    args = parse_args()
    subdivision_rows = read_subdivisions(args.subdivision_source_xml)
    if len(subdivision_rows) < 5_000:
        raise ValueError(
            "expected at least 5,000 CLDR subdivisions, "
            f"found {len(subdivision_rows)}"
        )
    write_subdivisions(args.subdivision_output, subdivision_rows)

    country_codes = read_country_codes(args.countries)
    country_alias_rows = read_country_aliases(
        args.territory_source_xml,
        country_codes,
    )
    if len(country_alias_rows) < 250:
        raise ValueError(
            "expected at least 250 CLDR country aliases, "
            f"found {len(country_alias_rows)}"
        )
    write_country_aliases(args.country_alias_output, country_alias_rows)

    print(
        f"wrote {len(subdivision_rows)} CLDR {CLDR_VERSION} subdivisions to "
        f"{args.subdivision_output}"
    )
    print(
        f"wrote {len(country_alias_rows)} CLDR {CLDR_VERSION} country aliases "
        f"to {args.country_alias_output}"
    )


if __name__ == "__main__":
    main()
