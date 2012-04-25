CC = g++
CCOPTS = -c -g -Wall -ggdb -funsigned-char
LINKOPTS = -g -pthread

all: controller

controller: controller.o \
	common_utils.o
	$(CC) $(LINKOPTS) -o $@ $^

common_utils.o: common_utils.cpp common_utils.h
	$(CC) $(CCOPTS) -o $@ common_utils.cpp

controller.o: controller.cpp controller.h
	$(CC) $(CCOPTS) -o $@ gateway_controller.cpp

clean:
	rm -rf *.o controller
