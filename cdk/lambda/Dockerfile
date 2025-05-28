FROM public.ecr.aws/lambda/python:3.12

COPY lambda/requirements.txt .
RUN pip install -r requirements.txt --target /asset

COPY lambda/lambda_function.py /asset/lambda_function.py

CMD ["lambda_function.lambda_handler"]
