# Dockerfile for Lambda Layer
FROM public.ecr.aws/lambda/python:3.12

# Install findutils for cleanup operations
RUN dnf update -y && \
    dnf install -y findutils && \
    dnf clean all

# Copy requirements file
COPY layer-requirements.txt /tmp/

# Install packages to the correct layer directory structure
# Lambda layers must be in /opt/python/ for Python packages
RUN pip install -r /tmp/layer-requirements.txt --target /opt/python \
    --no-cache-dir \
    --compile

# Optional: Safe cleanup to reduce layer size
RUN cd /opt/python && \
    # Remove test directories
    find . -type d -name 'tests' -exec rm -rf {} + 2>/dev/null || true && \
    find . -type d -name 'test' -exec rm -rf {} + 2>/dev/null || true && \
    find . -name '*test*.py' -path '*/tests/*' -delete 2>/dev/null || true && \
    # Remove __pycache__ directories
    find . -type d -name '__pycache__' -exec rm -rf {} + 2>/dev/null || true && \
    # Remove .pyc files (will be regenerated)
    find . -name '*.pyc' -delete && \
    find . -name '*.pyo' -delete && \

# The layer will be packaged from /opt/