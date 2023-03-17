FROM reg.huiwang.io/fat/tron-base:2
COPY tronapi /work/tronapi
COPY ./conf/config.yml  /work/conf/
COPY main.py /work/main.py
CMD ["python main.py"]

