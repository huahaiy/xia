# Summarization Evaluation

This is a developer-side harness for comparing local summarization models on a
small Xia-specific fixture suite. It is not part of Xia's shipped runtime.

The harness is aimed at the exact workload we care about for local documents:

- chunk summaries at ingest
- hierarchical document summaries built from chunk summaries
- CPU-friendly local Datalevin `llama.cpp` models

## What It Evaluates

The built-in fixture suite lives in `dev-resources/eval/summarization-fixtures.edn`.
It includes both:

- chunk-level source passages
- document-level samples with natural chunk boundaries

Each sample carries:

- one or more reference summaries
- required phrases that should survive compression
- forbidden phrases that should not appear

## Built-In Models

Built-in model ids:

- `extractive-baseline`
- `xia-default-local-summarizer`
- `qwen2.5-0.5b-instruct-gguf`
- `llama-3.2-1b-instruct-gguf`
- `smollm2-1.7b-instruct-gguf`

Only `xia-default-local-summarizer` is fully self-contained. The other GGUF
candidates are intentionally path-driven so you can point the harness at local
files with `--model-path`.

In the Xia product, model-based document summarization is opt-in. When enabled,
the managed default points at the larger Gemma 3 4B instruct GGUF rather than
the tiny Gemma 270M model.

## Commands

List known models:

```bash
clojure -M:summarization-eval --list-models
```

List known datasets:

```bash
clojure -M:summarization-eval --list-datasets
```

Run the built-in suite against the default Xia summarizer and a couple of local
GGUF candidates:

```bash
clojure -M:summarization-eval \
  --dataset xia-local-doc-fixtures \
  --models extractive-baseline,xia-default-local-summarizer,qwen2.5-0.5b-instruct-gguf,llama-3.2-1b-instruct-gguf \
  --model-path qwen2.5-0.5b-instruct-gguf=/models/Qwen2.5-0.5B-Instruct-Q4_K_M.gguf \
  --model-path llama-3.2-1b-instruct-gguf=/models/Llama-3.2-1B-Instruct-Q4_K_M.gguf \
  --include-samples \
  --output target/summarization-report.edn
```

## Metrics

The harness reports mean:

- `:required-coverage`
- `:forbidden-violation-rate`
- `:reference-token-precision`
- `:reference-token-recall`
- `:reference-token-f1`
- `:budget-pass-rate`
- `:budget-overrun-chars`
- `:compression-ratio`
- `:latency-ms`

Metrics are reported overall and split by sample kind (`:chunk` vs
`:document`).

## Notes

- `extractive-baseline` is useful as the floor: if a local model cannot beat
  it reliably, it should not be Xia's default.
- The default cache root is `~/.cache/xia/summarization-eval` on macOS/Linux,
  or `%LOCALAPPDATA%\\xia\\summarization-eval` on Windows. Override it with
  `--cache-dir` or `XIA_SUMMARIZATION_EVAL_CACHE_DIR`.
- Keep the fixture suite small and opinionated. This harness is meant to guide
  Xia product choices, not to become a generic benchmark zoo.
