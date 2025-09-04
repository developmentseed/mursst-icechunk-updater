FROM public.ecr.aws/lambda/python:3.12

# Install findutils package
RUN dnf update -y && \
    dnf install -y findutils && \
    dnf clean all

COPY lambda/requirements.txt .
RUN pip install -r requirements.txt --target /asset

# Reduce package size and remove useless files
RUN cd /asset && find . -type f -name '*.pyc' | while read f; do n=$(echo $f | sed 's/__pycache__\///' | sed 's/.cpython-[0-9]*//'); cp $f $n; done;
RUN cd /asset && find . -type d -a -name '__pycache__' -print0 | xargs -0 rm -rf
RUN cd /asset && find . -type f -a -name '*.py' -print0 | xargs -0 rm -f
RUN find /asset -type d -a -name 'tests' -print0 | xargs -0 rm -rf
RUN rm -rdf /asset/numpy/doc/ /asset/bin /asset/geos_license /asset/Misc

COPY aws_lambda/lambda_function.py /asset/lambda_function.py

CMD ["lambda_function.lambda_handler"]
