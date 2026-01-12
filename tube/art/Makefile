.PHONY: deepfuzz quantfuzz

deepfuzz:
	go test -timeout=0 -count=1 -fuzz=FuzzARTDeep -run=blah |tee fuzz.deep.log

quantfuzz:
	go test -timeout=0 -count=1 -fuzz=FuzzAscendDescend -run=blah |tee fuzz.ad.log

