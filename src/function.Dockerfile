# Dockerfile for Lambda Function (now lightweight)
FROM public.ecr.aws/lambda/python:3.12

# Only install function-specific dependencies (if any)
# Most dependencies are now in the layer
# COPY function-requirements.txt /tmp/
# RUN pip install -r /tmp/function-requirements.txt --target /asset --no-cache-dir || echo "No function-specific requirements"

# Copy your application code
RUN mkdir -p /asset/src
COPY src/updater.py /asset/src/updater.py
COPY src/settings.py /asset/src/settings.py
COPY src/lambda_function.py /asset/lambda_function.py

CMD ["lambda_function.lambda_handler"]