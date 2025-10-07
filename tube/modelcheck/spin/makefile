.PHONY: all unopt raft

all: # raft
	rm -f pan pan.c pan.b pan.h pan.m pan.p pan.t raft.pml.trail log.pan
	spin -a raft.pml
	##gcc -O2 -DSAFETY -DREACH -o pan pan.c
	##gcc -O2 -DSAFETY -o pan pan.c
	gcc -O2 -DSAFETY -DBITSTATE -o pan pan.c ## can give 30x less memory,30x faster
	./pan -E | tee log.pan  ## pan -i -E to find minimal example
	@(! test -f raft.pml.trail || (./pan -r raft.pml.trail ; false))
	@grep "Warning:\ Search\ not\ completed" log.pan || echo "good: search was completed."

optpax: # optimized version
	rm -f pan pan.c pan.b pan.h pan.m pan.p pan.t optimized.pml.trail log.pan
	spin -a optimized.pml
	##gcc -O2 -DSAFETY -DREACH -o pan pan.c
	##gcc -O2 -DSAFETY -o pan pan.c
	gcc -O2 -DSAFETY -DBITSTATE -o pan pan.c ## can give 30x less memory,30x faster
	./pan -E | tee log.pan  ## pan -i -E to find minimal example
	@(! test -f optimized.pml.trail || (./pan -r optimized.pml.trail ; false))
	@grep "Warning:\ Search\ not\ completed" log.pan || echo "good: search was completed."

unopt: # un-optimized version (see 'all', above, for much faster version)
	rm -f pan pan.c pan.b pan.h pan.m pan.p pan.t unopt.pml.trail log.pan
	spin -a unopt.pml
	##gcc -O2 -DSAFETY -DREACH -o pan pan.c
	##gcc -O2 -DSAFETY -o pan pan.c
	gcc -O2 -DSAFETY -DBITSTATE -o pan pan.c ## 482x less memory, 20% faster
	./pan -E | tee log.pan  ## pan -i -E to find minimal example
	@(! test -f unopt.pml.trail || (./pan -r unopt.pml.trail ; false))
	@grep "Warning:\ Search\ not\ completed" log.pan || echo "good: search was completed."

rflag: # optimized version
	rm -f pan pan.c pan.b pan.h pan.m pan.p pan.t rflag.pml.trail log.pan
	spin -a rflag.pml
	##gcc -O2 -DSAFETY -DREACH -o pan pan.c
	##gcc -O2 -DSAFETY -o pan pan.c
	gcc -O2 -DSAFETY -DBITSTATE -o pan pan.c
	./pan -E | tee log.pan  ## pan -i -E to find minimal example
	@(! test -f rflag.pml.trail || (./pan -r rflag.pml.trail ; false))
	@grep "Warning:\ Search\ not\ completed" log.pan || echo "good: search was completed."
