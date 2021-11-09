build_cpp:
	mpicc -c tasks.c
	mpicc -c utils.c
	mpic++ -o a03 main.cpp helpers.cpp tasks.o utils.o

build_test:
	gcc -c tasks.c
	gcc -c utils.c
	g++ -o test test.cpp tasks.o utils.o
