FROM public.ecr.aws/lambda/python:3.12

# Install findutils package
RUN dnf update -y && \
    dnf install -y findutils && \
    dnf clean all

COPY aws_lambda/requirements.txt .

# Install packages with optimization flags
RUN pip install -r requirements.txt --target /asset \
    --no-cache-dir \
    --compile \
    --no-deps-check-hash-checking

# Remove development and test files
RUN cd /asset && \
    # Remove test directories
    find . -type d -name 'tests' -exec rm -rf {} + 2>/dev/null || true && \
    find . -type d -name 'test' -exec rm -rf {} + 2>/dev/null || true && \
    find . -type d -name '*tests*' -exec rm -rf {} + 2>/dev/null || true && \
    # Remove documentation
    find . -type d -name 'doc' -exec rm -rf {} + 2>/dev/null || true && \
    find . -type d -name 'docs' -exec rm -rf {} + 2>/dev/null || true && \
    find . -type d -name 'examples' -exec rm -rf {} + 2>/dev/null || true && \
    find . -type d -name 'benchmarks' -exec rm -rf {} + 2>/dev/null || true && \
    # Remove __pycache__ directories
    find . -type d -name '__pycache__' -exec rm -rf {} + 2>/dev/null || true && \
    # Remove .pyc files
    find . -name '*.pyc' -delete && \
    find . -name '*.pyo' -delete && \
    # Remove .py files (keep .pyc compiled versions)
    find . -name '*.py' -delete && \
    # Remove specific large directories that aren't needed at runtime
    rm -rf bin/ geos_license/ Misc/ && \
    rm -rf numpy/doc/ numpy/tests/ && \
    rm -rf pandas/tests/ pandas/plotting/tests/ && \
    rm -rf dask/tests/ && \
    rm -rf xarray/tests/ && \
    rm -rf zarr/tests/ && \
    # Remove .dist-info directories (metadata)
    find . -name '*.dist-info' -exec rm -rf {} + 2>/dev/null || true && \
    # Remove .egg-info directories
    find . -name '*.egg-info' -exec rm -rf {} + 2>/dev/null || true && \
    # Remove locale files
    find . -path '*/locale/*' -name '*.po' -delete 2>/dev/null || true && \
    find . -path '*/locale/*' -name '*.pot' -delete 2>/dev/null || true && \
    # Remove typing stub files
    find . -name '*.pyi' -delete && \
    # Strip binaries if any
    find . -name '*.so' -exec strip {} + 2>/dev/null || true

# Copy your Lambda function
COPY aws_lambda/lambda_function.py /asset/lambda_function.py

CMD ["lambda_function.lambda_handler"]