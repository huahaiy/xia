# Embedding Evaluation

This is a developer-side evaluation harness for comparing Datalevin-compatible
embedding models on retrieval datasets. It is not part of Xia's shipped runtime
or release artifact.

The harness is designed around the same local runtime assumptions Xia uses in
production:

- text is embedded through Datalevin providers
- retrieval is run through a Datalevin vector index
- reports are emitted as EDN or JSON

## What It Evaluates

The current harness is aimed at retrieval tasks such as:

- `SciDocs`
- `SciFact`
- `FiQA`

Datasets are expected in a BEIR-style layout:

```text
DATASET_ROOT/
  corpus.jsonl
  queries.jsonl
  qrels/
    test.tsv
```

File expectations:

- `corpus.jsonl`: one JSON object per line with `_id`, `title`, and `text`
- `queries.jsonl`: one JSON object per line with `_id` and `text`
- `qrels/test.tsv`: tab-separated `query-id`, `corpus-id`, `score`

## Model Registry

Built-in model ids:

- `datalevin-default`
- `multilingual-e5-large-instruct-gguf`
- `qwen3-embedding-0.6b-gguf`
- `gte-multilingual-base-gguf`

Only `datalevin-default` is fully self-contained. The GGUF candidates expect a
local model file via `--model-path`, or a matching filename under the chosen
cache dir.

## Commands

List known models:

```bash
clojure -M:embedding-eval --list-models
```

List known datasets:

```bash
clojure -M:embedding-eval --list-datasets
```

Download the standard BEIR zips for SciDocs, SciFact, and FiQA into the local
eval cache:

```bash
clojure -M:embedding-eval --download-datasets scidocs-beir,scifact-beir,fiqa-beir
```

Run a comparison on SciDocs:

```bash
clojure -M:embedding-eval \
  --dataset scidocs-beir \
  --models datalevin-default,multilingual-e5-large-instruct-gguf,qwen3-embedding-0.6b-gguf \
  --model-path multilingual-e5-large-instruct-gguf=/models/multilingual-e5-large-instruct.gguf \
  --model-path qwen3-embedding-0.6b-gguf=/models/qwen3-embedding-0.6b.gguf \
  --top-k 1,5,10 \
  --output target/scidocs-embedding-report.edn
```

Run a comparison on SciFact:

```bash
clojure -M:embedding-eval \
  --dataset scifact-beir \
  --models datalevin-default,qwen3-embedding-0.6b-gguf \
  --model-path qwen3-embedding-0.6b-gguf=/models/qwen3-embedding-0.6b.gguf \
  --format json \
  --output target/scifact-embedding-report.json
```

## Metrics

The harness currently reports mean:

- `recall@k`
- `mrr@k`
- `ndcg@k`

along with:

- corpus count
- query count
- embedding dimensions
- indexing/search timing

## Notes

- If `--dataset-path` is omitted and the dataset registry entry has download
  metadata, the harness downloads and caches the dataset automatically.
- The default cache root is `~/.cache/xia/embedding-eval` on macOS/Linux, or
  `%LOCALAPPDATA%\\xia\\embedding-eval` on Windows. Override it with
  `--cache-dir` or `XIA_EMBEDDING_EVAL_CACHE_DIR`.
- Query embeddings are produced with Datalevin `:kind :query`.
- Document embeddings are produced with Datalevin `:kind :document`.
- That means E5/Qwen models can apply their query/document formatting rules
  through Datalevin provider metadata.
