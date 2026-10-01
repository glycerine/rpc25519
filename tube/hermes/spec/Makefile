.PHONY: prove fuzz clean

all: clean prove fuzz

prove:
	XTRACE_OFF=1 ivy_check epocher.ivy
	#XTRACE_OFF=1 goivy_check epocher.ivy

fuzz:
	#XTRACE_OFF=1 ivy_to_cpp target=test build=true epocher.ivy
	XTRACE_OFF=1 ivy2cpp target=test build=true epocher.ivy
	./epocher iters=10

gofuzz: clean
	XTRACE_OFF=1 ivy2golang target=test build=true epocher.ivy
	./epocher iters=10

clean:
	rm -rf epocher epocher.cpp epocher.dSYM epocher.dsc epocher.h epocher.dsc epocher.go
	rm -rf timestamp timestamp.cpp timestamp.dSYM timestamp.dsc timestamp.go timestamp.h

ts:
	#XTRACE_OFF=1 goivy_check timestamp.ivy
	#XTRACE_OFF=1 ivy_check timestamp.ivy
	XTRACE_OFF=1 ivy_to_cpp target=test build=true isolate=test_timestamp timestamp.ivy
	#XTRACE_OFF=1 ivy2cpp target=test build=true isolate=test_timestamp timestamp.ivy
	#XTRACE_OFF=1 ivy2golang target=test build=true debug=assert isolate=test_timestamp timestamp.ivy
	./timestamp iters=5
