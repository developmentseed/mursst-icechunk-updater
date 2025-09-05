# Dockerfile for AWS Lambda Layer
FROM public.ecr.aws/lambda/python:3.12 as builder

# Install minimal system deps (skip full dnf update)
RUN dnf install -y findutils && dnf clean all

# Copy requirements
COPY src/layer-requirements.txt /tmp/requirements.txt

# Install Python deps into /opt/python
RUN pip install -r /tmp/requirements.txt \
    --target /opt/python \
    --no-cache-dir \
    --compile

# Strip unnecessary files
RUN cd /opt/python && \
    # Remove tests, examples, docs, caches
    find . -type d -name 'tests' -o -name 'test' -o -name 'testing' -o -name 'docs' -o -name 'examples' | xargs rm -rf && \
    # Remove caches, pyc, pyo
    find . -type d -name '__pycache__' -exec rm -rf {} + && \
    find . -type f -name '*.py[co]' -delete && \
    # Drop .dist-info heavy metadata except entry points + version
    find . -type d -name '*.dist-info' | xargs -I {} sh -c \
      'rm -rf {}/LICENSE* {}/COPYING* {}/AUTHORS* {}/CHANGELOG* {}/docs {}/news {}/example* {}/tests' && \
    # Drop .egg-info entirely
    find . -type d -name '*.egg-info' -exec rm -rf {} + && \
    # Drop binaries not needed in Lambda (e.g. CLI wrappers)
    rm -rf bin share include lib/*.a

# Package into /asset for CDK
RUN mkdir -p /asset/python && cp -r /opt/python/* /asset/python/
