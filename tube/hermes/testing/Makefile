
default: py

go:
	XTRACE_OFF=1 ivy2cpp target=test build=true hermes_rmw_o3_testing.ivy
	rm -rf goout && mkdir goout && cp hermes_rmw_o3_testing.*  goout/
	#./hermes_rmw_o3_testing

clean:
	rm -f hermes_rmw_o3_testing hermes_rmw_o3_testing.cpp hermes_rmw_o3_testing.h hermes_rmw_o3_testing.dsc

py:
	XTRACE_OFF=1 ivy_to_cpp target=test build=true hermes_rmw_o3_testing.ivy
	rm -rf pyout && mkdir pyout && cp hermes_rmw_o3_testing.*  pyout/
	#./hermes_rmw_o3_testing

punish:
	./hermes_rmw_o3_testing runs=1 iters=100 seed=1

check:
	XTRACE_OFF=1 time ivy_check diagnose=true trace=true hermes_rmw_o3_testing.ivy > log.ivy_check.out
	tail -4 log.ivy_check.out
