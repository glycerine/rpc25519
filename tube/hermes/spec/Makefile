.PHONY: prove fuzz clean

all: clean prove fuzz

prove:
	XTRACE_OFF=1 ivy_check member.ivy

fuzz:
	XTRACE_OFF=1 ivy_to_cpp target=test build=true member.ivy
	./member iters=10

clean:
	rm -rf member member.cpp member.dSYM member.dsc member.h member.dsc
