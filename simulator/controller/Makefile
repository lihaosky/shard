CC = gcc
CCOPTS = -c -g -Wall -ggdb -funsigned-char
LINKOPTS = -g -levent -lmemcached

all: controller

controller: controller.o \
	util.o
	$(CC) -o $@ $^ $(LINKOPTS)

util.o: util.c util.h
	$(CC) $(CCOPTS) -o $@ util.c

controller.o: controller.c
	$(CC) $(CCOPTS) -o $@ controller.c

clean:
	rm -rf *.o controller
