# Compgrapg package

Python 3.11.5

## How to install
```bash
$compgraph pip wheel --wheel-dir dist .

$compgraph pip install . --prefer-binary --force-reinstall --find-links dist

```

### Run tests
```bash
$compgraph pytest
```

### Run examples
```bash
$compgraph python3 compgraph/<example_file>.py --help

$compgraph python3 compgraph/<example_file>.py --input <input.txt> --output <output.txt>
```
