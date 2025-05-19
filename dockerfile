FROM public.ecr.aws/lambda/python:3.11

# Install redis client
RUN pip install redis

# Copy code
COPY app.py ${LAMBDA_TASK_ROOT}

# Set the handler
CMD ["app.lambda_handler"]
