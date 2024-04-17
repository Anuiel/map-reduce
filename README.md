# Compgrapg package

Python 3.11.5

## How to install
```bash
pip3 wheel --wheel-dir dist .

pip3 install . --prefer-binary --force-reinstall --find-links dist
```

### Run tests
```bash
$compgraph pytest
```

### Run examples
```bash
python3 examples/<example_file>.py --help

python3 examples/<example_file>.py --input <input.txt> --output <output.txt>
```
