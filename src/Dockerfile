[cite_start][cite: 1] FROM public.ecr.aws/lambda/python:3.9

RUN yum update -y && \
    yum install -y gcc python3-devel

COPY requirements.txt .
[cite_start][cite: 2] RUN pip install -r requirements.txt

RUN yum remove -y gcc python3-devel && \
    yum clean all

# Copiamos AMBOS archivos
COPY app.py ${LAMBDA_TASK_ROOT}
COPY signals.py ${LAMBDA_TASK_ROOT} 

# El CMD por defecto sigue siendo app.handler, pero lo sobreescribiremos en Terraform para el segundo lambda
CMD [ "app.handler" ]
