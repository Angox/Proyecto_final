FROM public.ecr.aws/lambda/python:3.9

RUN yum update -y && \
    yum install -y gcc python3-devel

COPY requirements.txt .
RUN pip install -r requirements.txt

RUN yum remove -y gcc python3-devel && \
    yum clean all

# Copiamos archivos de la aplicación
COPY app.py ${LAMBDA_TASK_ROOT}
COPY signals.py ${LAMBDA_TASK_ROOT} 

# Handler por defecto
CMD [ "app.handler" ]
