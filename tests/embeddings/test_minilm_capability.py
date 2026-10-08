"""The MiniLM embedding capability, end to end, with the REAL model.

Needs onnxruntime (`pip install --no-deps "onnxruntime>=1.30,<2"`) and the all-MiniLM-L6-v2 weights in
OPTERYX_MINILM_MODEL_DIR. Run with `make test-embeddings`; excluded from the default
collection (pyproject norecursedirs) because the weights are not shipped. Without them these
tests FAIL — they never skip.

Each test runs in a fresh interpreter: the capability is process-lifetime and refuses a
width change once planned, so it cannot share a process with the core 256-d static hash.
"""

import os
import subprocess
import sys

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))


def _run(body: str) -> str:
    code = (
        f"import sys; sys.path.insert(1, {ROOT!r})\n"
        "from opteryx.types.vectors.embedding_capability import install_minilm_capability\n"
        "cap = install_minilm_capability()\n"
        "import opteryx\n"
        "from draken.morsels.morsel import Morsel\n"
        "def one(sql):\n"
        "    return Morsel.combine(list(opteryx.session().execute_to_morsels(sql)))\n" + body
    )
    done = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, timeout=300)
    assert done.returncode == 0, done.stderr   # includes a clean process exit
    return done.stdout


def test_capability_installs_with_identity():
    out = _run("print(cap.name, cap.dimensions, cap.identity.split(':sha256:')[0])")
    assert out.split() == ["minilm-l6-v2", "384", "minilm-l6-v2:256"], out


def test_text_cosine_is_semantic():
    out = _run(
        "a = one(\"SELECT COSINE_SIMILARITY('dog', 'puppy') AS s\").column('s').to_pylist()[0]\n"
        "b = one(\"SELECT COSINE_SIMILARITY('dog', 'spreadsheet') AS s\").column('s').to_pylist()[0]\n"
        "print(a, b)"
    )
    related, unrelated = (float(x) for x in out.split())
    assert related > 0.7 and unrelated < 0.4 and related > unrelated, out


def test_match_uses_the_same_embedder():
    out = _run(
        "s = one(\"SELECT COSINE_SIMILARITY('red planet', 'Mars') AS s\").column('s').to_pylist()[0]\n"
        "print(s)"
    )
    assert float(out) > 0.3, out
