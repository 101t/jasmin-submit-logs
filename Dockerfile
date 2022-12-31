FROM jookies/jasmin:0.10.12

COPY requirements.txt .

RUN pip install -r requirements.txt

COPY sms_logger.py .

ENTRYPOINT [ "./sms_logger.py" ]
