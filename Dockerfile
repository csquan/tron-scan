FROM reg.huiwang.io/fat/tron-base:latest
COPY tronapi /work/tronapi
COPY config  /work/config
COPY main.py /work/main.py
CMD ["python main.py"]

