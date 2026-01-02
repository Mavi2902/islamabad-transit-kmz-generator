# KMZ Generator

This project generates KMZ (Keyhole Markup Language Zipped) files from GTFS (General Transit Feed Specification) data and integrates metro line data from a private GitHub repository.

## Setup

1.  **Clone the repository:**
    ```bash
    git clone <your-repository-url>
    cd "SHAPE FILE"
    ```

2.  **Create a virtual environment (recommended):**
    ```bash
    python -m venv venv
    .\venv\Scripts\activate # On Windows
    source venv/bin/activate # On macOS/Linux
    ```

3.  **Install dependencies:**
    ```bash
    pip install -r requirements.txt
    ```

## Environment Variables

The script requires the following environment variables to be set:

*   `KENTKART_GTFS_URL`: The URL to your GTFS feed (e.g., operator/city specific GTFS zip file).
*   `GITHUB_TOKEN`: A GitHub Personal Access Token (PAT) with `repo` scope to access the private metro lines repository.

**How to set environment variables (Windows):**

```cmd
set KENTKART_GTFS_URL="your_gtfs_url_here"
set GITHUB_TOKEN="your_github_token_here"
```

**How to set environment variables (macOS/Linux):**

```bash
export KENTKART_GTFS_URL="your_gtfs_url_here"
export GITHUB_TOKEN="your_github_token_here"
```

## How to Run

Once the environment variables are set and dependencies are installed, you can run the script:

```bash
python "kmz generator.py"
```

This will generate an `Islamabad Transit.kmz` file in the same directory.
