from __future__ import annotations

import json
from pathlib import Path


PROJECTS = ("personal_finance", "wremotely")


def load_manifest(project: str) -> dict[str, object]:
    path = Path(project) / "target" / "manifest.json"
    if not path.is_file():
        raise RuntimeError(f"missing parsed manifest: {path}")
    return json.loads(path.read_text())


def validate_manifest(project: str, manifest: dict[str, object]) -> None:
    metadata = manifest.get("metadata")
    if not isinstance(metadata, dict) or metadata.get("project_name") != project:
        raise RuntimeError(f"{project} manifest has the wrong project identity")

    resources: dict[str, object] = {}
    for collection_name in ("nodes", "sources"):
        collection = manifest.get(collection_name)
        if not isinstance(collection, dict):
            raise RuntimeError(f"{project} manifest is missing {collection_name}")
        resources.update(collection)

    if not resources:
        raise RuntimeError(f"{project} manifest has no graph resources")

    for unique_id, resource in resources.items():
        if not isinstance(resource, dict):
            raise RuntimeError(f"{project} manifest resource {unique_id} is invalid")
        if resource.get("package_name") != project:
            raise RuntimeError(
                f"{project} owns a resource from {resource.get('package_name')}: {unique_id}"
            )

        depends_on = resource.get("depends_on", {})
        if not isinstance(depends_on, dict):
            raise RuntimeError(f"{unique_id} has invalid dependency metadata")
        for dependency in depends_on.get("nodes", []):
            dependency_parts = dependency.split(".", 2)
            if len(dependency_parts) < 2 or dependency_parts[1] != project:
                raise RuntimeError(
                    f"cross-project graph dependency in {project}: "
                    f"{unique_id} -> {dependency}"
                )


def main() -> int:
    manifests = {project: load_manifest(project) for project in PROJECTS}
    for project, manifest in manifests.items():
        validate_manifest(project, manifest)

    resource_ids = {
        project: set(manifest["nodes"]) | set(manifest["sources"])
        for project, manifest in manifests.items()
    }
    overlap = resource_ids["personal_finance"] & resource_ids["wremotely"]
    if overlap:
        raise RuntimeError(f"dbt projects share resource IDs: {sorted(overlap)}")

    print("dbt domain project boundaries OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
