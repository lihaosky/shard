CC = g++
CCOPTS = -c -g -Wall -ggdb -funsigned-char
LINKOPTS = -g -pthread

all: gateway_controller

gateway_controller: gateway_controller.o \
	common_utils.o
	$(CC) $(LINKOPTS) -o $@ $^

common_utils.o: common_utils.cpp common_utils.h
	$(CC) $(CCOPTS) -o $@ common_utils.cpp

gateway_controller.o: gateway_controller.cpp gateway_controller.h
	$(CC) $(CCOPTS) -o $@ gateway_controller.cpp

clean:
	rm -rf *.o gateway_controller

cleandump:
	rm -rf
