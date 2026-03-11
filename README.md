# Little Maia2

The aim of the project is to create a light weight model for a probability chess move prediction model. The model focuses on predicting moves of lower rated players ranging from (1200 and below).

Getting an existing project up and running with `uv` is a breath of fresh air compared to the old `pip` and `venv` dance. It’s incredibly fast and handles the heavy lifting of Python version management for you.

Here is a clean, professional template you can drop straight into your `README.md`.

---

## Project Setup

Follow these steps to set up your local development environment using [uv](https://github.com/astral-sh/uv).

### 1. Prerequisites

Ensure you have `uv` installed on your machine. If you don't have it yet, run:

```bash
# macOS/Linux
curl -LsSf https://astral-sh.uv/install.sh | sh

# Windows (PowerShell)
powershell -c "irm https://astral-sh.uv/install.ps1 | iex"

```

### 2. Clone the Repository

```bash
git clone https://github.com/username/repository-name.git
cd repository-name

```

### 3. Initialize the Environment

`uv` will automatically detect the required Python version from the `.python-version` file or `pyproject.toml` and install it if necessary.

```bash
# Create a virtual environment and install all dependencies
uv sync

```

> **Note:** This command creates a `.venv` directory and ensures your lockfile is up to date with your environment.

### 4. Activate the Environment

To enter the virtual environment, use:

```bash
# macOS/Linux
source .venv/bin/activate

# Windows
.venv\Scripts\activate

```

### 5. Running the Project

Once activated, you can run your scripts directly:

```bash
python main.py

```

*Alternatively, you can run commands without activating the environment using:*
`uv run main.py`

---

### 💡 Quick Tips for `uv`

* **Add a new package:** `uv add package-name`
* **Remove a package:** `uv remove package-name`
* **Update dependencies:** `uv lock --update`

---

## Reference
**Links**:
- ***GitHub link***: https://github.com/CSSLab/maia2/tree/main
- ***Research paper***: https://arxiv.org/pdf/2409.20553

```bash
@inproceedings{
tang2024maia,
title={Maia-2: A Unified Model for Human-{AI} Alignment in Chess},
author={Zhenwei Tang and Difan Jiao and Reid McIlroy-Young and Jon Kleinberg and Siddhartha Sen and Ashton Anderson},
booktitle={The Thirty-eighth Annual Conference on Neural Information Processing Systems},
year={2024},
url={https://openreview.net/forum?id=XWlkhRn14K}
}
```
