FROM public.ecr.aws/lambda/python:3.9

# 1. Instalamos gcc y las cabeceras de desarrollo de python
# Esto es necesario para compilar librerías como pycares o pandas si no encuentran binarios
RUN yum update -y && \
    yum install -y gcc python3-devel

COPY requirements.txt .

# 2. Instalamos las librerías de Python
RUN pip install -r requirements.txt

# 3. (Opcional pero recomendado) Limpiamos las herramientas de compilación 
# para que la imagen final pese menos, ya que solo se necesitan para el "build"
RUN yum remove -y gcc python3-devel && \
    yum clean all

COPY app.py ${LAMBDA_TASK_ROOT}

CMD [ "app.handler" ]
