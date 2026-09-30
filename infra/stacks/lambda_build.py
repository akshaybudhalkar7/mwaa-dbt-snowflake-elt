"""Build Lambda code folders at synth time - no Docker needed (works on Windows and in CI).

Dependencies are installed as Linux x86_64 wheels (--platform), whatever OS runs `cdk synth`.
A fingerprint of all inputs is stored in the folder, so it is rebuilt only when something changes.
"""

import hashlib
import shutil
import subprocess
import sys
from pathlib import Path

BUILD_ROOT = Path(__file__).resolve().parents[1] / ".lambda_build"
LAMBDA_PYTHON = "3.12"
# Lambda python3.12 runs on Amazon Linux 2023 (glibc 2.34) -> any manylinux wheel up to 2_34 works.
# Listed explicitly: older pip versions don't expand one tag to the older compatible ones.
LAMBDA_PLATFORMS = ("manylinux2014_x86_64", "manylinux_2_17_x86_64", "manylinux_2_28_x86_64", "manylinux_2_34_x86_64")


def python_asset(name: str, files: dict[str, Path], requirements: Path | None = None) -> str:
    """Folder with `files` ({path inside the zip: source file}) + `requirements` installed.

    Returns the folder path for lambda_.Code.from_asset().
    """
    digest = hashlib.sha256(f"{LAMBDA_PYTHON}|{LAMBDA_PLATFORMS}".encode())
    for dest, src in sorted(files.items()):
        digest.update(dest.encode())
        digest.update(Path(src).read_bytes())
    if requirements:
        digest.update(Path(requirements).read_bytes())
    fingerprint = digest.hexdigest()

    out = BUILD_ROOT / name
    marker = out / ".fingerprint"
    if marker.exists() and marker.read_text() == fingerprint:
        return str(out)

    shutil.rmtree(out, ignore_errors=True)
    out.mkdir(parents=True)
    if requirements:
        subprocess.run(
            [
                sys.executable, "-m", "pip", "install", "--quiet",
                "--requirement", str(requirements),
                "--target", str(out),
                *(arg for platform in LAMBDA_PLATFORMS for arg in ("--platform", platform)),
                "--implementation", "cp",
                "--python-version", LAMBDA_PYTHON,
                "--only-binary=:all:",  # required with --platform: never compile for the wrong OS
                "--no-compile",  # no .pyc -> same inputs give identical files -> no needless redeploys
            ],
            check=True,
        )
    for dest, src in files.items():
        target = out / dest
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(src, target)

    marker.write_text(fingerprint)
    return str(out)
