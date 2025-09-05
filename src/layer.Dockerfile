# Dockerfile for Lambda Layer
FROM public.ecr.aws/lambda/python:3.12

# Install findutils for cleanup operations
RUN dnf update -y && \
    dnf install -y findutils && \
    dnf clean all

# Copy requirements file
COPY src/layer-requirements.txt /tmp/

# Install packages into Lambda layer path
RUN pip install -r /tmp/layer-requirements.txt \
    --target /opt/python \
    --no-cache-dir \
    --compile

# Cleanup: remove bytecode, tests, docs, and other unnecessary files
RUN find /opt/python -type f -name '*.pyc' -delete && \
    find /opt/python -type d -name '__pycache__' -exec rm -rf {} + && \
    find /opt/python -type f -name '*.pyo' -delete && \
    find /opt/python -type d -name 'tests' -exec rm -rf {} + && \
    rm -rf /opt/python/numpy/doc \
           /opt/python/bin \
           /opt/python/geos_license \
           /opt/python/Misc


# The layer will be packaged from /opt/

# 👇 CDK expects /asset-output or /asset
RUN mkdir -p /asset/python
COPY --from=0 /opt/python /asset/python