clean:
	rm -f hermes_rmw_o3_testing hermes_rmw_o3_testing.cpp hermes_rmw_o3_testing.h hermes_rmw_o3_testing.dsc

py:
	XTRACE_OFF=1 ivy_to_cpp target=test build=true hermes_rmw_o3_testing.ivy
	rm -rf pyout && mkdir pyout && cp hermes_rmw_o3_testing.*  pyout/
	./hermes_rmw_o3_testing

go:
	XTRACE_OFF=1 ivy2cpp target=test build=true hermes_rmw_o3_testing.ivy
	rm -rf goout && mkdir goout && cp hermes_rmw_o3_testing.*  goout/
	./hermes_rmw_o3_testing
