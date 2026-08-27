"""Embeddings: fastembed, BAAI/bge-small-en-v1.5, 384 dimensions.

fastembed rather than sentence-transformers: it runs the model through ONNX
Runtime with no torch dependency, which keeps the image small enough that the
`core` profile stays under a gigabyte and makes CPU-only inference the normal
path rather than a degraded one.

384 dimensions is a frozen number, not a tuning knob. It is written into the
`vector(384)` column in rag.chunks, so changing the model to one with a
different width is a schema migration; the check in `assert_dimensions` turns
that from a confusing insert error into a sentence that says so.
"""

from __future__ import annotations

from functools import lru_cache

import numpy as np

# CONTRACTS.md §1 pins the pgvector column width to this.
EMBEDDING_DIMENSIONS = 384

# BGE v1.5 was trained with an asymmetric objective: passages are embedded bare,
# short queries are embedded behind this instruction. The model card recommends
# it for retrieval, and fastembed's `query_embed()` does NOT apply it — for this
# model it returns a vector identical to `embed()`, verified rather than
# assumed. So it is applied here, explicitly, where it can be seen and measured.
# `make rag-eval` reports recall with and without it.
BGE_QUERY_INSTRUCTION = "Represent this sentence for searching relevant passages: "


class Embedder:
    """Wraps one fastembed model.

    The model is loaded lazily. Importing this module must stay free: the test
    suite imports the RAG package to check masking, and a 67 MB download at
    import time would make `pytest` depend on the network.
    """

    def __init__(self, model_name: str, *, query_instruction: str = BGE_QUERY_INSTRUCTION) -> None:
        self.model_name = model_name
        self.query_instruction = query_instruction
        self._model = None

    @property
    def model(self):
        if self._model is None:
            from fastembed import TextEmbedding  # imported here, see docstring

            self._model = TextEmbedding(model_name=self.model_name)
        return self._model

    def embed_documents(self, texts: list[str], batch_size: int = 64) -> list[np.ndarray]:
        """Embed passages. No instruction prefix — that is the asymmetry."""
        if not texts:
            return []
        vectors = list(self.model.embed(texts, batch_size=batch_size))
        assert_dimensions(vectors[0], self.model_name)
        return vectors

    def embed_query(self, text: str) -> np.ndarray:
        vector = list(self.model.embed([self.query_instruction + text]))[0]
        assert_dimensions(vector, self.model_name)
        return vector

    def embed_queries(self, texts: list[str], batch_size: int = 64) -> list[np.ndarray]:
        if not texts:
            return []
        prefixed = [self.query_instruction + t for t in texts]
        vectors = list(self.model.embed(prefixed, batch_size=batch_size))
        assert_dimensions(vectors[0], self.model_name)
        return vectors


def assert_dimensions(vector: np.ndarray, model_name: str) -> None:
    if vector.shape[-1] != EMBEDDING_DIMENSIONS:
        raise RuntimeError(
            f"{model_name} produces {vector.shape[-1]}-dimensional vectors, but "
            f"rag.chunks.embedding is vector({EMBEDDING_DIMENSIONS}). Changing the "
            f"embedding model is a schema migration, not a config change."
        )


@lru_cache(maxsize=4)
def get_embedder(model_name: str, query_instruction: str = BGE_QUERY_INSTRUCTION) -> Embedder:
    """Cached per model name — loading the ONNX session twice is pure waste."""
    return Embedder(model_name, query_instruction=query_instruction)
