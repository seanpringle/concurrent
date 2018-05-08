all:
	gcc -Wall -g -O2 -std=c99 -o concurrent concurrent.c

clean:
	-rm concurrent
